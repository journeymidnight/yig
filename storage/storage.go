package storage

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	"log"
	"path/filepath"
	"sync"

	"legitlab.letv.cn/yig/yig/api/datatype"
	. "legitlab.letv.cn/yig/yig/error"
	"legitlab.letv.cn/yig/yig/helper"
	"legitlab.letv.cn/yig/yig/meta"
)

const (
	CEPH_CONFIG_PATTERN          = "conf/*.conf"
	AES_BLOCK_SIZE               = 16
	ENCRYPTION_KEY_LENGTH        = 32 // key size for AES-"256"
	INITIALIZATION_VECTOR_LENGTH = 16 // block size of AES
)

var (
	RootContext = context.Background()
)

// *YigStorage implements api.ObjectLayer
type YigStorage struct {
	DataStorage map[string]*CephStorage
	DataCache   DataCache
	MetaStorage *meta.Meta
	Logger      *log.Logger
	Stopping    bool
	WaitGroup   *sync.WaitGroup
}

func New(logger *log.Logger, cacheEnabled bool) *YigStorage {
	metaStorage := meta.New(logger, cacheEnabled)
	yig := YigStorage{
		DataStorage: make(map[string]*CephStorage),
		DataCache:   newDataCache(cacheEnabled),
		MetaStorage: metaStorage,
		Logger:      logger,
		Stopping:    false,
		WaitGroup:   new(sync.WaitGroup),
	}

	cephConfs, err := filepath.Glob(CEPH_CONFIG_PATTERN)
	if err != nil {
		panic("No ceph conf found")
	}

	for _, conf := range cephConfs {
		c := NewCephStorage(conf, logger)
		yig.DataStorage[c.Name] = c
	}

	initializeRecycler(&yig)
	return &yig
}

func (y *YigStorage) Stop() {
	y.Stopping = true
	helper.Logger.Print("Stopping storage...")
	y.WaitGroup.Wait()
	helper.Logger.Println("done")
}

func encryptionKeyFromSseRequest(sseRequest datatype.SseRequest) (encryptionKey []byte, err error) {

	switch sseRequest.Type {
	case "": // no encryption
		return nil, nil
	case "KMS":
		return nil, nil // not implemented yet
	case "S3":
		encryptionKey = make([]byte, ENCRYPTION_KEY_LENGTH)
		_, err = io.ReadFull(rand.Reader, encryptionKey)
		if err != nil {
			return
		}
	case "C":
		encryptionKey = sseRequest.SseCustomerKey
	default:
		err = ErrInvalidSseHeader
		return
	}

	return
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
