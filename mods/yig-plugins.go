package mods

import (
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
	NUMS_PLUGIN
)

func InitialPlugins() map[string]*YigPlugin {

	globalPlugins := make(map[string]*YigPlugin)
	var sopath string

	for name, pluginConfig := range helper.CONFIG.Plugins {
		sopath = pluginConfig.Path
		helper.Logger.Printf(5, "plugins: open for %s\n", name)
		if pluginConfig.Path == "" {
			helper.Logger.Printf(5, "plugin path for %s is empty\n", name)
			continue
		}


		//if enable do not exist in toml file, enable's default is false
		if pluginConfig.Enable == false {
			helper.Logger.Printf(5, "plugins: %s is not enabled, continue\n", sopath)
			continue
		}

		//open plugin file
		plug, err := plugin.Open(sopath)
		if err != nil {
			helper.Logger.Printf(5, "plugins: failed to open %s for %s", sopath, name)
			continue
		}
		exported, err := plug.Lookup(EXPORTED_PLUGIN)
		if err != nil {
			helper.Logger.Printf(5, "plugins: lookup %s in %s failed, err: %v\n", EXPORTED_PLUGIN, sopath, err)
			continue
		}

		//check plugin type
		yigPlugin, ok := exported.(*YigPlugin)
		if !ok {
			helper.Logger.Printf(5, "plugins: convert %s in %s failed, exported: %v\n", EXPORTED_PLUGIN, sopath, exported)
			continue
		}

		//check plugin content
		if yigPlugin.Name == name && yigPlugin.Create != nil {
			globalPlugins[yigPlugin.Name] = yigPlugin
		} else {
			helper.Logger.Printf(5, "plugins: check %s failed, value: %v\n", sopath, yigPlugin)
			continue
		}
		helper.Logger.Printf(10, "plugins: loaded plugin %s from %s\n", yigPlugin.Name, sopath)
	}

	return globalPlugins
}
