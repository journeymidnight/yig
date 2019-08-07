package main

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/mods"
	"net/http"
)


const pluginName = "cdn_judge"

var Exported = mods.YigPlugin{
	Name:       pluginName,
	PluginType: mods.JUDGE_PLUGIN,
	Create:  GetJudgeClient,
}


func GetJudgeClient(config map[string]interface{}) (interface{}, error) {

	helper.Logger.Printf(10, "Get plugin config: %v\n", config)

	c := JudgeClient{
		JudgeCdnTarget: config["target"].(string),
	}

	return interface{}(c), nil
}

type JudgeClient struct {
	JudgeCdnTarget string
}

func (j JudgeClient) JudgeCDNRequest(r *http.Request) bool {
	cdnFlag, ok := r.URL.Query()[j.JudgeCdnTarget]
	if ok && len(cdnFlag) > 0 && cdnFlag[0] == "cdn" {
		return true
	}
	return false
}
