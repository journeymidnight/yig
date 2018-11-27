package _go

type Config struct {
	AccessKey string `ini:"access_key"`
	SecretKey string `ini:"secret_key"`
	EndPoint  string `ini:"host_base"`
}


// Load the config file if possible, but if there is an error return the default configuration file
func loadConfigFile(path string) (*Config, error) {
	config := &Config{
		AccessKey: "hehehehe",
		SecretKey: "hehehehe",
		EndPoint:  "s3.test.com",
	}
	return config, nil
}
