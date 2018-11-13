package _go

import (
	"os"
	"testing"
)

func Test_ReadConfig(t *testing.T) {
	f, err := os.Open("/etc/yig/yig.json")
	if err != nil {
		t.Fatal("Cannot open /etc/yig/yig.json")
		panic(err)
	}
	defer f.Close()

	conf := GetDefaultConfigPath()
	t.Logf("Get config path: %s", conf)
	c, err := loadConfigFile(conf)
	if err != nil {
		t.Fatal(err)
		panic(err)
	}
	t.Logf("config: %+v", c)
}
