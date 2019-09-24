package storage

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"github.com/journeymidnight/yig/backend"
	"io"
	"path"
	"sync"
	"time"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/circuitbreak"
	"github.com/journeymidnight/yig/crypto"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta"
	"github.com/journeymidnight/yig/redis"
)

const (
	AES_BLOCK_SIZE               = 16
	ENCRYPTION_KEY_LENGTH        = 32 // key size for AES-"256"
	INITIALIZATION_VECTOR_LENGTH = 16 // block size of AES
)

// *YigStorage implements api.ObjectLayer
type YigStorage struct {
	DataStorage map[string]backend.Cluster
	DataCache   DataCache
	MetaStorage *meta.Meta
	KMS         crypto.KMS
	Logger      *log.Logger
	Stopping    bool
	WaitGroup   *sync.WaitGroup
}

func (y *YigStorage) Stop() {
	y.Stopping = true
	helper.Logger.Print(5, "Stopping storage...")
	y.WaitGroup.Wait()
	helper.Logger.Println(5, "done")
}

// check cache health per one second if enable cache
func (y *YigStorage) PingCache(interval time.Duration) {
	tick := time.NewTicker(interval)
	for {
		select {
		case <-tick.C:
			redis.CacheCircuit.Execute(
				context.Background(),
				func(ctx context.Context) (err error) {
					c, err := redis.GetClient(ctx)
					if err != nil {
						return err
					}
					defer c.Close()
					// Use table.String() + key as Redis key
					_, err = c.Do("PING")
					helper.ErrorIf(err, "Cmd: %s.", "PING")
					return err
				},
				nil,
			)
			if redis.CacheCircuit.IsOpen() {
				helper.Logger.Println(10, circuitbreak.CacheCircuitIsOpenErr)
			}
		}
	}
}

func (yig *YigStorage) encryptionKeyFromSseRequest(sseRequest datatype.SseRequest, bucket, object string) (key []byte, encKey []byte, err error) {
	switch sseRequest.Type {
	case "": // no encryption
		return nil, nil, nil
	// not implemented yet
	case crypto.S3KMS.String():
		return nil, nil, ErrNotImplemented
	case crypto.S3.String():
		if yig.KMS == nil {
			return nil, nil, ErrKMSNotConfigured
		}
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
