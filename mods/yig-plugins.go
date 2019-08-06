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

const (
	EXPORTED_PLUGIN = "Exported"
	EXPORTEDCDN_PLUGIN = "Exportedcdn"
)

const (
	IAM_PLUGIN = iota //IamClient interface
	JUDGE_PLUGIN
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
		if name == "dummy_iam" {
			//if enable do not exist in toml file, enable's default is false
			if pluginConfig.Enable == false {
				helper.Logger.Printf(5, "plugins: %s is not enabled, continue\n", sopath)
				continue
			}

			//open plugin file
			plug, err := plugin.Open(sopath)
			helper.Logger.Println(10,"plaugin.so:",sopath)
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
			yigPluginIAM, ok := exported.(*YigPlugin)
			if !ok {
				helper.Logger.Printf(5, "plugins: convert %s in %s failed, exported: %v\n", EXPORTED_PLUGIN, sopath, exported)
				continue
			}

			helper.Logger.Println(10,"yigPluginJudge.Name: %s  Name: %s",yigPluginIAM.Name,name)
			helper.Logger.Println(10,"yigPluginJudge:",yigPluginIAM)
			//check plugin content
			if yigPluginIAM.Name == name && yigPluginIAM.Create != nil {
				globalPlugins[yigPluginIAM.Name] = yigPluginIAM
			} else {
				helper.Logger.Printf(5, "plugins: check %s failed, value: %v\n", sopath, yigPluginIAM)
				continue
			}
			helper.Logger.Printf(10, "plugins: loaded plugin %s from %s\n", yigPluginIAM.Name, sopath)
		} else if name == "dummy_judge" {
			//if enable do not exist in toml file, enable's default is false
			if pluginConfig.Enable == false {
				helper.Logger.Printf(5, "plugins: %s is not enabled, continue\n", sopath)
				continue
			}
			plug, err := plugin.Open(sopath)
			helper.Logger.Println(10,"plaugin.so:",sopath)
			if err != nil {
				helper.Logger.Printf(5, "plugins: failed to open %s for %s", sopath, name)
				continue
			}
			exportedcdn, err := plug.Lookup(EXPORTEDCDN_PLUGIN)
			if err != nil {
				helper.Logger.Printf(5, "plugins: lookup %s in %s failed, err: %v\n", EXPORTEDCDN_PLUGIN, sopath, err)
				continue
			}

			yigPluginJudge, ok := exportedcdn.(*YigPlugin)
			if !ok {
				helper.Logger.Printf(5, "plugins: convert %s in %s failed, exported: %v\n", EXPORTEDCDN_PLUGIN, sopath, exportedcdn)
				continue
			}
			helper.Logger.Println(10,"yigPluginJudge.Name: %s  Name: %s",yigPluginJudge.Name,name)
			helper.Logger.Println(10,"yigPluginJudge:",yigPluginJudge)
			if yigPluginJudge.Name == name && yigPluginJudge.Create != nil {
				globalPlugins[yigPluginJudge.Name] = yigPluginJudge
			} else {
				helper.Logger.Printf(5, "plugins: check %s failed, value: %v\n", sopath, yigPluginJudge)
				continue
			}
			helper.Logger.Printf(10, "plugins: loaded plugin %s from %s\n", yigPluginJudge.Name, sopath)
		}
	}

	return globalPlugins
}
