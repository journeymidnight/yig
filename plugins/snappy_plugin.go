package main

import (
	"github.com/golang/snappy"
	. "github.com/journeymidnight/yig/mods"
	"io"
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

func (s SnappyCompress) CompressWriter(writer io.Writer) io.Writer {
	snappyWriter := snappy.NewBufferedWriter(writer)
	return snappyWriter
}