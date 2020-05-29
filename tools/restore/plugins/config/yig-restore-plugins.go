package config

import (
	"fmt"
	"github.com/journeymidnight/yig-restore/helper"
	"plugin"
)

type YigPlugin struct {
	Name       string
	PluginType int
	Create     func(map[string]interface{}) (interface{}, error)
}

const EXPORTED_PLUGIN = "Exported"

const (
	IAM_PLUGIN = iota //IamClient interface
	MQ_PLUGIN
	COMPRESS_PLUGIN
	NUMS_PLUGIN
)

func InitialPlugins() map[string]*YigPlugin {

	globalPlugins := make(map[string]*YigPlugin)
	var sopath string

	for name, pluginConfig := range helper.Conf.Plugins {
		sopath = pluginConfig.Path
		helper.Logger.Info("plugins: open for", name)
		if pluginConfig.Path == "" {
			helper.Logger.Info("plugin path for", name, "is empty")
			continue
		}

		//if enable do not exist in toml file, enable's default is false
		if pluginConfig.Enable == false {
			helper.Logger.Info(sopath, "is not enabled, continue")
			continue
		}

		//open plugin file
		plug, err := plugin.Open(sopath)
		if err != nil {
			helper.Logger.Error(fmt.Sprintf(
				"plugins: failed to open %s for %s, err: %v\n",
				sopath, name, err))
			continue
		}
		exported, err := plug.Lookup(EXPORTED_PLUGIN)
		if err != nil {
			helper.Logger.Error(fmt.Sprintf(
				"plugins: lookup %s in %s failed, err: %v\n",
				EXPORTED_PLUGIN, sopath, err))
			continue
		}

		//check plugin type
		yigPlugin, ok := exported.(*YigPlugin)
		if !ok {
			helper.Logger.Warn(fmt.Sprintf(
				"plugins: convert %s in %s failed, exported: %v\n",
				EXPORTED_PLUGIN, sopath, exported))
			continue
		}

		//check plugin content
		if yigPlugin.Name == name && yigPlugin.Create != nil {
			globalPlugins[yigPlugin.Name] = yigPlugin
		} else {
			helper.Logger.Warn(fmt.Sprintf(
				"plugins: check %s failed, value: %v\n", sopath, yigPlugin))
			continue
		}
		helper.Logger.Info(fmt.Sprintf(
			"plugins: loaded plugin %s from %s\n", yigPlugin.Name, sopath))
	}

	return globalPlugins
}
