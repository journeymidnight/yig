package _go

import (
	"testing"
)

func Test_ReadConfig(t *testing.T) {
	c, err := loadConfigFile()
	if err != nil {
		t.Fatal(err)
		panic(err)
	}
	t.Logf("config: %+v", c)
}
