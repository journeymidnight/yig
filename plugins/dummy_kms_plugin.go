package main

import (
	"github.com/journeymidnight/yig/crypto"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/mods"
)

const (
	pluginName    = "dummy_encryption_kms"
	plaintextKey  = "qwertyuiopasdfghjklzxcvbnmaaaaaa"
	ciphertextKey = "mnbvcxzlkjhgfdsapoiuytrewqzzzzzz"
)

//The variable MUST be named as Exported.
//the code in yig-plugin will lookup this symbol
var Exported = mods.YigPlugin{
	Name:       pluginName,
	PluginType: mods.KMS_PLUGIN,
	Create:     GetDummyKMSClient,
}

func GetDummyKMSClient(config map[string]interface{}) (interface{}, error) {
	helper.Logger.Info("Get KMS plugin config:", config)
	c := &DummyKMS{
		KMSUrl: config["url"].(string),
	}

	return c, nil
}

type DummyKMS struct {
	KMSUrl string
}

func (d *DummyKMS) GenerateKey(keyName string, context crypto.Context) (key [32]byte, sealedKey []byte, err error) {
	helper.Logger.Info("Generate key succeed! plaintext:", plaintextKey, " ciphertext: ", ciphertextKey)
	copy(key[:], []byte(plaintextKey))
	return key, []byte(ciphertextKey), nil
}

func (d *DummyKMS) UnsealKey(keyName string, sealedKey []byte, context crypto.Context) (key [32]byte, err error) {
	helper.Logger.Info("Unseal key succeed! sealedKey:", string(sealedKey), " ciphertext:", ciphertextKey)
	copy(key[:], []byte(plaintextKey))
	return key, nil
}

func (d *DummyKMS) GetKeyID() string {
	return "yig"
}
