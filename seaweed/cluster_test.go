package seaweed

import (
	"bytes"
	"github.com/journeymidnight/yig/log"
	"io/ioutil"
	"os"
	"testing"
)

func prepareClient() SeaweedfsCluster {
	logger := log.New(os.Stdout, "",
		log.Ldate|log.Ltime|log.Lmicroseconds, 0)
	seaweedMasters := []string{
		"10.5.0.23:10001",
	}
	return NewSeaweedStorage(logger, "test-client", seaweedMasters)
}

func TestStorage(t *testing.T) {
	client := prepareClient()
	// PUT
	content := []byte("hehe, seaweedfs")
	contentReader := bytes.NewReader(content)
	objectName, written, err := client.Put("", contentReader)
	if int(written) != len(content) || err != nil {
		t.Error("written", written, "len(content)", len(content),
			"err", err)
	}
	// GET
	reader, err := client.GetReader("", objectName, 0, 0)
	if err != nil {
		t.Error("GetReader error:", err)
	}
	readContent, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Error("ReadAll error:", err)
	}
	if string(readContent) != string(content) {
		t.Error("readContent != content",
			string(readContent), string(content))
	}
	// Remove
	err = client.Remove("", objectName)
	if err != nil {
		t.Error("Remove error:", err)
	}
}
