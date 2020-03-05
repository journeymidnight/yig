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

func (d DummyCompress) Compress(reader io.Reader) (result io.Reader, err error) {
	return reader, nil
}

func (d DummyCompress) UnCompress(reader io.Reader) (result io.Reader, err error) {
	return reader, nil
}

func (d DummyCompress) IsCompressible(objectName, mtype string) bool {
	return true
}
