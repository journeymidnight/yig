package storage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"git.letv.cn/yig/yig/api/datatype"
	. "git.letv.cn/yig/yig/error"
	"git.letv.cn/yig/yig/meta"
	"io"
	"log"
	"path/filepath"
)

const (
	CEPH_CONFIG_PATTERN          = "conf/*.conf"
	ENCRYPTION_KEY_LENGTH        = 32 // for AES-"256"
	INITIALIZATION_VECTOR_LENGTH = 12 // best performance for GCM
)

// *YigStorage implements minio.ObjectLayer
type YigStorage struct {
	DataStorage map[string]*CephStorage
	MetaStorage *meta.Meta
	Logger      *log.Logger
	// TODO
}

func New(logger *log.Logger) *YigStorage {
	metaStorage := meta.New(logger)
	yig := YigStorage{
		DataStorage: make(map[string]*CephStorage),
		MetaStorage: metaStorage,
		Logger:      logger,
	}

	cephConfs, err := filepath.Glob(CEPH_CONFIG_PATTERN)
	if err != nil {
		panic("No ceph conf found")
	}

	for _, conf := range cephConfs {
		c := NewCephStorage(conf, logger)
		yig.DataStorage[c.Name] = c
	}

	return &yig
}

func keysFromSseRequest(sseRequest datatype.SseRequest) (encryptionKey []byte,
	initializationVector []byte, err error) {

	switch sseRequest.Type {
	case "": // no encryption
		return
	case "KMS":
		return // not implemented yet
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

	initializationVector = make([]byte, INITIALIZATION_VECTOR_LENGTH)
	_, err = io.ReadFull(rand.Reader, initializationVector)
	if err != nil {
		return
	}

	return
}

// Wraps reader with encryption if encryptionKey is not empty
func wrapEncryptionReader(reader io.Reader, encryptionKey []byte,
	initializationVector []byte) (wrappedReader io.Reader, err error) {

	wrappedReader = reader

	if len(encryptionKey) != 0 {
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
	}
	return
}
