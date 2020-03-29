package compression

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/mods"
	"io"
)

type Compression interface {
	CompressReader(reader io.Reader) io.Reader
	CompressWriter(writer io.Writer) io.Writer
	IsCompressible(objectName, mtype string) bool
}

var Compress Compression

// create the Compression
func InitCompression(plugins map[string]*mods.YigPlugin) (Compression, error) {
	for name, p := range plugins {
		if p.PluginType == mods.COMPRESS_PLUGIN {
			c, err := p.Create(helper.CONFIG.Plugins[name].Args)
			if err != nil {
				helper.Logger.Error("failed to initial Compression plugin:", name, "\nerr:", err)
				return nil, err
			}
			helper.Logger.Println("Compression plugin is", name)
			Compress = c.(Compression)
			return Compress, nil
		}
	}
	panic("Failed to initialize any Compression plugin, quiting...\n")
}

func CompressObject(objectName, objectType string, reader io.Reader) io.Reader {
	if helper.CONFIG.EnableCompression {
		isCompressible := Compress.IsCompressible(objectName, objectType)
		if !isCompressible {
			return reader
		} else {
			return Compress.CompressReader(reader)
		}
	}
	return reader
}
