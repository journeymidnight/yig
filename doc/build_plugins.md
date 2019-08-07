# How to build plugin

## Preflight

You should have installed `docker` in your environment.

## Write your code

Please write your plugin code in `${YIG_DIR}/plugins/`

Note that the package must be `main` !

For example to write plugin of IAM :
```go
// This name is applied to the parsing of the configuration file.
// You need to use this name to resolve the corresponding method.
const pluginName = "dummy_iam"

// The variable MUST be named as Exported.
// We specify to use this name to encapsulate the plugin method.
// the code in yig-plugin will lookup this symbol
var Exported = mods.YigPlugin{
	Name:       pluginName,
	PluginType: mods.IAM_PLUGIN,
	Create:  GetIamClient,
}

//Prepare the parameters you need to read in the configuration file here.
func GetIamClient(config map[string]interface{}) (interface{}, error) {

	helper.Logger.Printf(10, "Get plugin config: %v\n", config)

	c := DebugIamClient{
		IamUrl: config["url"].(string),
	}

	return interface{}(c), nil
}

//Used to implement the interface
type DebugIamClient struct {
	IamUrl string
}

//The following function is the function method you need to implement plug-in
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
# Plugin Config list.The name must match the one you edited
[plugins.dummy_iam]
path = "/etc/yig/plugins/dummy_iam_plugin.so"
enable = true
# Here are the parameters you need to load
[plugins.dummy_iam.args]
url = "s3.test.com"
```
## Add your plugin in `yig-plugins` (If you do not need fast reuse, skip this step)
```go
$cd ${YIG_DIR}/mods
$vi yig-plugins
//Add your plugin to the structure for fast reuse
const (
	IAM_PLUGIN = iota //IamClient interface
	NUMS_PLUGIN
)
```

## Restart yig
You can use your plugin elegantly in yig
