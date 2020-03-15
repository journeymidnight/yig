package main

import (
	"github.com/golang/snappy"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/mods"
	"io"
	"strings"
	"sync"
)

const pluginName = "snappy"

//The variable MUST be named as Exported.
//the code in yig-plugin will lookup this symbol
var Exported = YigPlugin{
	Name:       pluginName,
	PluginType: COMPRESS_PLUGIN,
	Create:     GetCompressClient,
}

var downloadBufPool sync.Pool

func GetCompressClient(config map[string]interface{}) (interface{}, error) {
	snappy := SnappyCompress{}
	return snappy, nil
}

type SnappyCompress struct{}

func (s SnappyCompress) CompressReader(reader io.Reader) io.Reader {
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		defer pipeWriter.Close()
		downloadBufPool.New = func() interface{} {
			return make([]byte, helper.CONFIG.DownloadBufPoolSize)
		}
		buffer := downloadBufPool.Get().([]byte)
		_, err := io.CopyBuffer(snappy.NewBufferedWriter(pipeWriter), reader, buffer)
		downloadBufPool.Put(buffer)
		if err != nil {
			helper.Logger.Error("Unable to read an object need compress:", err)
			pipeWriter.CloseWithError(err)
			return
		}
	}()
	return pipeReader
}

func (s SnappyCompress) CompressWriter(writer io.Writer) io.Writer {
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		defer pipeReader.Close()
		downloadBufPool.New = func() interface{} {
			return make([]byte, helper.CONFIG.DownloadBufPoolSize)
		}
		buffer := downloadBufPool.Get().([]byte)
		_, err := io.CopyBuffer(writer, pipeReader, buffer)
		downloadBufPool.Put(buffer)
		if err != nil {
			helper.Logger.Error("Unable to read an object need compress:", err)
			pipeWriter.CloseWithError(err)
			return
		}
	}()
	return pipeWriter
}

func (s SnappyCompress) IsCompressible(objectName, mtype string) bool {
	objectNameSlice := strings.Split(objectName, ".")
	str := objectNameSlice[len(objectNameSlice)-1]
	suffix := "." + str

	// text
	if strings.HasPrefix(mtype, "text/") {
		return true
	}

	// images
	switch suffix {
	case ".svg", ".bmp":
		return true
	}
	if strings.HasPrefix(mtype, "image/") {
		return false
	}

	// by file name extension
	switch suffix {
	case ".zip", ".rar", ".gz", ".bz2", ".xz":
		return false
	case ".pdf", ".txt", ".html", ".htm", ".css", ".js", ".json":
		return true
	case ".php", ".java", ".go", ".rb", ".c", ".cpp", ".h", ".hpp":
		return true
	case ".png", ".jpg", ".jpeg":
		return false
	}

	// by mime type
	if strings.HasPrefix(mtype, "application/") {
		if strings.HasSuffix(mtype, "xml") {
			return true
		}
		if strings.HasSuffix(mtype, "script") {
			return true
		}
	}

	return false
}
