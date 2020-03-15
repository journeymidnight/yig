package main

import (
	. "github.com/journeymidnight/yig/mods"
	"io"
)

const pluginName = "dummy_compression"

//The variable MUST be named as Exported.
//the code in yig-plugin will lookup this symbol
var Exported = YigPlugin{
	Name:       pluginName,
	PluginType: COMPRESS_PLUGIN,
	Create:     GetDummyCompressClient,
}

func GetDummyCompressClient(config map[string]interface{}) (interface{}, error) {
	dummy := DummyCompress{}
	return dummy, nil
}

type DummyCompress struct{}

func (d DummyCompress) CompressReader(reader io.Reader) io.Reader {
	return reader
}

func (d DummyCompress) CompressWriter(writer io.Writer) io.Writer {
	return writer
}

func (d DummyCompress) IsCompressible(objectName, mtype string) bool {
	return true
}
