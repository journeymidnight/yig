package crypto

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/mods"
)

var kms KMS

func NewKMS(plugins map[string]*mods.YigPlugin) KMS {
	for name, p := range plugins {
		if p.PluginType == mods.KMS_PLUGIN {
			c, err := p.Create(helper.CONFIG.Plugins[name].Args)
			if err != nil {
				helper.Logger.Error("failed to initial KMS plugin:", name, "\nerr:", err)
				return nil
			}
			helper.Logger.Println("Message KMS plugin is", name)
			kms = c.(KMS)
			return kms
		}
	}
	helper.Logger.Info("not support kms type")
	panic("Failed to initialize any KMS plugin, quiting...\n")
}
