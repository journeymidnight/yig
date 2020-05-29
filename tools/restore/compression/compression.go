package compression

import (
	"github.com/journeymidnight/yig-restore/helper"
	. "github.com/journeymidnight/yig-restore/plugins/config"
	"io"
)

type Compression interface {
	CompressReader(reader io.Reader) io.Reader
	UnCompressReader(reader io.Reader) io.Reader
	IsCompressible(objectName, mtype string) bool
}

var Compress Compression

// create the Compression
func InitCompression(plugins map[string]*YigPlugin) (Compression, error) {
	for name, p := range plugins {
		if p.PluginType == COMPRESS_PLUGIN {
			c, err := p.Create(helper.Conf.Plugins[name].Args)
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

func UnCompressObject(objectName, objectType string, reader io.Reader) io.Reader {
	if helper.Conf.EnableCompression {
		isCompressible := Compress.IsCompressible(objectName, objectType)
		if !isCompressible {
			return reader
		} else {
			return Compress.UnCompressReader(reader)
		}
	}
	return reader
}
