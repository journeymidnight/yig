package mods

import (
	"fmt"
	"plugin"

	"github.com/journeymidnight/yig/helper"
)

/*YigPlugin is the exported variable from plugins.
* the Name here is defined in plugin's code. the plugin is opened when config file is set.
* the PluginType is for different interface type
* such as:
* IAM_PLUGIN => IamClient interface
* UNKNOWN_PLUGIN=> other interface
 */
type YigPlugin struct {
	Name       string
	PluginType int
	Create     func(map[string]interface{}) (interface{}, error)
}

const EXPORTED_PLUGIN = "Exported"

const (
	IAM_PLUGIN = iota //IamClient interface
	MESSAGEBUS_PLUGIN
	NUMS_PLUGIN
)

func InitialPlugins() map[string]*YigPlugin {

	globalPlugins := make(map[string]*YigPlugin)
	var sopath string

	for name, pluginConfig := range helper.CONFIG.Plugins {
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
