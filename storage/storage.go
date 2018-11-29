package storage

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	"path/filepath"
	"sync"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/crypto"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta"
	"path"
)

const (
	AES_BLOCK_SIZE               = 16
	ENCRYPTION_KEY_LENGTH        = 32 // key size for AES-"256"
	INITIALIZATION_VECTOR_LENGTH = 16 // block size of AES
	DEFAULT_CEPHCONFIG_PATTERN   = "conf/*.conf"
)

var (
	RootContext = context.Background()
)

// *YigStorage implements api.ObjectLayer
type YigStorage struct {
	DataStorage map[string]*CephStorage
	DataCache   DataCache
	MetaStorage *meta.Meta
	KMS         crypto.KMS
	Logger      *log.Logger
	Stopping    bool
	WaitGroup   *sync.WaitGroup
}

func New(logger *log.Logger, metaCacheType int, enableDataCache bool, CephConfigPattern string) *YigStorage {
	metaStorage := meta.New(logger, meta.CacheType(metaCacheType))
	kms := crypto.NewKMS()
	yig := YigStorage{
		DataStorage: make(map[string]*CephStorage),
		DataCache:   newDataCache(enableDataCache),
		MetaStorage: metaStorage,
		KMS:         kms,
		Logger:      logger,
		Stopping:    false,
		WaitGroup:   new(sync.WaitGroup),
	}
	if CephConfigPattern == "" {
		CephConfigPattern = DEFAULT_CEPHCONFIG_PATTERN
	}

	cephConfs, err := filepath.Glob(CephConfigPattern)
	helper.Logger.Printf(5, "Reading Ceph conf files from %+v\n", cephConfs)
	if err != nil || len(cephConfs) == 0 {
		helper.Logger.Panic(0, "PANIC: No ceph conf found")
	}

	for _, conf := range cephConfs {
		c := NewCephStorage(conf, logger)
		if c != nil {
			yig.DataStorage[c.Name] = c
		}
	}

	initializeRecycler(&yig)
	return &yig
}

func (y *YigStorage) Stop() {
	y.Stopping = true
	helper.Logger.Print(5, "Stopping storage...")
	y.WaitGroup.Wait()
	helper.Logger.Println(5, "done")
}

func (yig *YigStorage) encryptionKeyFromSseRequest(sseRequest datatype.SseRequest, bucket, object string) (key []byte, encKey []byte, err error) {
	switch sseRequest.Type {
	case "": // no encryption
		return nil, nil, nil
	// not implemented yet
	case crypto.S3KMS.String():
		return nil, nil, ErrNotImplemented
	case crypto.S3.String():
		key, encKey, err := yig.KMS.GenerateKey(yig.KMS.GetKeyID(), crypto.Context{bucket: path.Join(bucket, object)})
		if err != nil {
			return nil, nil, err
		}
		return key[:], encKey, nil
	case crypto.SSEC.String():
		return sseRequest.SseCustomerKey, nil, nil
	default:
		err = ErrInvalidSseHeader
		return
	}
}

func newInitializationVector() (initializationVector []byte, err error) {

	initializationVector = make([]byte, INITIALIZATION_VECTOR_LENGTH)
	_, err = io.ReadFull(rand.Reader, initializationVector)
	return
}

// Wraps reader with encryption if encryptionKey is not empty
func wrapEncryptionReader(reader io.Reader, encryptionKey []byte,
	initializationVector []byte) (wrappedReader io.Reader, err error) {

	if len(encryptionKey) == 0 {
		return reader, nil
	}

	var block cipher.Block
	block, err = aes.NewCipher(encryptionKey)
	if err != nil {
		return
	}
	stream := cipher.NewCTR(block, initializationVector)
	wrappedReader = cipher.StreamReader{
		S: stream,
		R: reader,
	}
	return
}

type alignedReader struct {
	aligned bool // indicate whether alignment has already been done
	offset  int64
	reader  io.Reader
}

func (r *alignedReader) Read(p []byte) (n int, err error) {
	if r.aligned {
		return r.reader.Read(p)
	}

	r.aligned = true
	buffer := make([]byte, len(p))
	n, err = r.reader.Read(buffer)
	if err != nil {
		return
	}

	n = copy(p, buffer[r.offset:n])
	return
}

// AES is a block cipher with block size of 16 bytes, i.e. the basic unit of encryption/decryption
// is 16 bytes. As an HTTP range request could start from any byte, we need to read one more
// block if necessary.
// Also, our chosen mode of operation for YIG is CTR(counter), which features parallel
// encryption/decryption and random read access. We need all these three features, this leaves
// us only three choices: ECB, CTR, and GCM.
// ECB is best known for its insecurity, meanwhile the GCM implementation of golang(as in 1.7) discourage
// users to encrypt large files in one pass, which requires us to read the whole file into memory. So
// the implement complexity is similar between GCM and CTR, we choose CTR because it's faster(but more
// prone to man-in-the-middle modifications)
//
// See https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation
// and http://stackoverflow.com/questions/39347206
func wrapAlignedEncryptionReader(reader io.Reader, startOffset int64, encryptionKey []byte,
	initializationVector []byte) (wrappedReader io.Reader, err error) {

	if len(encryptionKey) == 0 {
		return reader, nil
	}

	alignedOffset := startOffset / AES_BLOCK_SIZE * AES_BLOCK_SIZE
	newReader, err := wrapEncryptionReader(reader, encryptionKey, initializationVector)
	if err != nil {
		return
	}
	if alignedOffset == startOffset {
		return newReader, nil
	}

	wrappedReader = &alignedReader{
		aligned: false,
		offset:  startOffset - alignedOffset,
		reader:  newReader,
	}
	return
}
