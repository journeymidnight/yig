package main

import (
	"bytes"
	"github.com/golang/snappy"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/mods"
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

func (s SnappyCompress) CompressWriter(input []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	w := snappy.NewBufferedWriter(buf)
	if _, err := w.Write(input); err != nil {
		helper.Logger.Error("error compressing data:", err)
		return nil, err
	}
	if err := w.Close(); err != nil {
		helper.Logger.Error("error closing compressed data:", err)
		return nil, err
	}
	return buf.Bytes(), nil
}
