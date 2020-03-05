package main

import (
	"bytes"
	"github.com/golang/snappy"
	. "github.com/journeymidnight/yig/mods"
	"io"
	"strings"
)

const pluginName = "snappy"

//The variable MUST be named as Exported.
//the code in yig-plugin will lookup this symbol
var Exported = YigPlugin{
	Name:       pluginName,
	PluginType: COMPRESS_PLUGIN,
	Create:     GetCompressClient,
}

func GetCompressClient(config map[string]interface{}) (interface{}, error) {
	snappy := SnappyCompress{}
	return snappy, nil
}

type SnappyCompress struct{}

// TODO : Fix memory issues
func (s SnappyCompress) Compress(reader io.Reader) (result io.Reader, err error) {
	var out, input []byte
	for {
		inputNum, err := reader.Read(input)
		if err != nil && err != io.EOF {
			return reader, err
		}
		if inputNum == 0 {
			break
		}
		out = snappy.Encode(nil, input)
	}
	result = bytes.NewReader(out)
	return result, nil
}

// TODO : Fix memory issues
func (s SnappyCompress) UnCompress(reader io.Reader) (result io.Reader, err error) {
	var out, input []byte
	for {
		inputNum, err := reader.Read(input)
		if err != nil && err != io.EOF {
			return reader, err
		}
		if inputNum == 0 {
			break
		}
		out, err = snappy.Decode(nil, input)
		if err != nil {
			return reader, err
		}
	}
	result = bytes.NewReader(out)
	return result, nil
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
