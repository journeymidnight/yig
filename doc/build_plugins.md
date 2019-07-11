# How to build plugin

## Preflight

You should have installed `docker` in your environment.

## Write yout code

Please write your plugin code in `${YIG_DIR}/plugins/`

Note that the package must be `main` !

For example to write plugin of IAM :
```go
// package must be `main`
package main

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/iam/common"
)

type DebugIamClient struct {
	IamUrl string
}

// implement the interface of `IamClient`
func (d DebugIamClient) GetKeysByUid(uid string) (c []common.Credential, err error) {
	return
}

func (d DebugIamClient) GetCredential(accessKey string) (c common.Credential, err error) {
	return common.Credential{
		UserId:          "hehehehe",
		DisplayName:     "hehehehe",
		AccessKeyID:     accessKey,
		SecretAccessKey: "hehehehe",
	}, nil // For test now
}

// export the function to get the IamClient you have implemented.
func GetIamClient() (c iam.IamClient, err error) {
	// Get config data
	data := helper.CONFIG.Plugins[iam.IamPluginName].Data
	helper.Logger.Println(20, "Get plugin data:", data)
	c = DebugIamClient{
		IamUrl: data["url"].(string),
	}
	return
}
```

## Build plugins

```
cd ${YIG_DIR}
make plugin
```

The `.so` files of your all plugins will generate in ${YIG_DIR}/plugins/

## Copy the plugin files to your specified path

```
cp ${YIG_DIR}/plugins/*.so /etc/yig/
```

## Add your plugin config in `yig.toml`

```
# Plugin Config
[plugins]
    # IAM plugin path
    [plugins.iam]
    path = "/etc/yig/plugins/iam_plugin.so"
        # IAM plugin parameters
        [plugins.iam.data]
        url = "s3.iam.com"
```

## Restart yig
