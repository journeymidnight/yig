package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/journeymidnight/yig/crypto"
	"github.com/journeymidnight/yig/mods"
	"strings"
	"time"

	vault "github.com/hashicorp/vault/api"
	"github.com/journeymidnight/yig/helper"
)

const (
	pluginName           = "encryption_vault"
	DEBUG_ROOT_TOKEN     = "myroot"
	DEBUG_LEASE_DURATION = 60 * 60 * 24 * 30 // 30 days
)

var (
	//ErrKMSAuthLogin is raised when there is a failure authenticating to KMS
	ErrKMSAuthLogin = errors.New("Vault service did not return auth info")
)

type vaultService struct {
	config        *VaultConfig
	client        *vault.Client
	leaseDuration time.Duration
}

// return transit secret engine's path for generate data key operation
func (v *vaultService) genDataKeyEndpoint(key string) string {
	return "/transit/datakey/plaintext/" + key
}

// return transit secret engine's path for decrypt operation
func (v *vaultService) decryptEndpoint(key string) string {
	return "/transit/decrypt/" + key
}

// VaultKey represents vault encryption key-id name & version
type VaultKey struct {
	Name    string `json:"name"`
	Version int64  `json:"version"`
}

// VaultAuth represents vault auth type to use. For now, AppRole is the only supported
// auth type.
type VaultAuth struct {
	Type    string       `json:"type"`
	AppRole VaultAppRole `json:"approle"`
}

// VaultAppRole represents vault approle credentials
type VaultAppRole struct {
	ID     string `json:"id"`
	Secret string `json:"secret"`
}

// VaultConfig holds config required to start vault service
type VaultConfig struct {
	Endpoint string    `json:"endpoint"`
	Auth     VaultAuth `json:"auth"`
	Key      VaultKey  `json:"key-id"`
}

//The variable MUST be named as Exported.
//the code in yig-plugin will lookup this symbol
var Exported = mods.YigPlugin{
	Name:       pluginName,
	PluginType: mods.KMS_PLUGIN,
	Create:     GetVaultClient,
}

func GetVaultClient(config map[string]interface{}) (interface{}, error) {
	helper.Logger.Info("Get KMS plugin config:", config)

	c, err := NewVaultConfig(config)
	if err != nil {
		panic("read kms vault err:" + err.Error())
	}
	vault, err := NewVault(c)
	if err != nil {
		panic("create vault err:" + err.Error())
	}
	return vault, nil
}

// validate whether all required env variables needed to start vault service have
// been set
func validateVaultConfig(c *VaultConfig) error {
	if c.Endpoint == "" {
		return fmt.Errorf("Missing hashicorp vault endpoint - %s is empty", "c.Endpoint")
	}
	if strings.ToLower(c.Auth.Type) != "approle" {
		return fmt.Errorf("Unsupported hashicorp vault auth type - %s", "c.Auth.Type")
	}
	if c.Auth.AppRole.ID == "" {
		return fmt.Errorf("Missing hashicorp vault AppRole ID - %s is empty", "c.Auth.AppRole.ID")
	}
	if c.Auth.AppRole.Secret == "" {
		return fmt.Errorf("Missing hashicorp vault AppSecret ID - %s is empty", "c.Auth.AppRole.Secret")
	}
	if c.Key.Name == "" {
		return fmt.Errorf("Invalid value set in environment variable %s", "c.Key.Name")
	}
	if c.Key.Version < 0 {
		return fmt.Errorf("Invalid value set in environment variable %s", "c.Key.Version")
	}
	return nil
}

// authenticate to vault with app role id and app role secret, and get a client access token, lease duration
func getVaultAccessToken(client *vault.Client, appRoleID, appSecret string) (token string, duration int, err error) {
	data := map[string]interface{}{
		"role_id":   appRoleID,
		"secret_id": appSecret,
	}
	resp, e := client.Logical().Write("auth/approle/login", data)
	if e != nil {
		return token, duration, e
	}
	if resp.Auth == nil {
		return token, duration, ErrKMSAuthLogin
	}
	return resp.Auth.ClientToken, resp.Auth.LeaseDuration, nil
}

// NewVaultConfig sets KMSConfig from environment
// variables and performs validations.
func NewVaultConfig(config map[string]interface{}) (VaultConfig, error) {
	vault := VaultConfig{
		Endpoint: config["endpoint"].(string),
		Auth: VaultAuth{
			Type: "approle",
			AppRole: VaultAppRole{
				ID:     config["kms_id"].(string),
				Secret: config["kms_secret"].(string),
			},
		},
		Key: VaultKey{
			Version: config["version"].(int64),
			Name:    config["keyName"].(string),
		},
	}

	if err := validateVaultConfig(&vault); err != nil {
		return vault, err
	}
	return vault, nil
}

// NewVault initializes Hashicorp Vault KMS by
// authenticating to Vault with the credentials in KMSConfig,
// and gets a client token for future api calls.
func NewVault(vaultConf VaultConfig) (interface{}, error) {
	vconfig := &vault.Config{
		Address: vaultConf.Endpoint,
	}

	c, err := vault.NewClient(vconfig)
	if err != nil {
		return nil, err
	}

	var accessToken string
	var leaseDuration int
	if helper.CONFIG.DebugMode == true {
		accessToken = DEBUG_ROOT_TOKEN
		leaseDuration = DEBUG_LEASE_DURATION
	} else {
		accessToken, leaseDuration, err = getVaultAccessToken(c, vaultConf.Auth.AppRole.ID, vaultConf.Auth.AppRole.Secret)
		helper.Logger.Info("Get access token:", accessToken,
			"lease duration:", leaseDuration)
		if err != nil {
			return nil, err
		}
	}

	// authenticate and get the access token
	helper.Logger.Info("Get vault token:", accessToken, "leaseDuration", leaseDuration)
	c.SetToken(accessToken)
	v := vaultService{client: c, config: &vaultConf, leaseDuration: time.Duration(leaseDuration)}
	v.renewToken(c)
	return &v, nil
}

func (v *vaultService) renewToken(c *vault.Client) {
	retryDelay := 1 * time.Minute
	go func() {
		for {
			s, err := c.Auth().Token().RenewSelf(int(v.leaseDuration))
			if err != nil {
				time.Sleep(retryDelay)
				continue
			}
			nextRenew := s.Auth.LeaseDuration / 2
			time.Sleep(time.Duration(nextRenew) * time.Second)
		}
	}()
}

// Generates a random plain text key, sealed plain text key from
// Vault. It returns the plaintext key and sealed plaintext key on success
func (v *vaultService) GenerateKey(keyID string, ctx crypto.Context) (key [32]byte, sealedKey []byte, err error) {
	contextStream := new(bytes.Buffer)
	ctx.WriteTo(contextStream)

	payload := map[string]interface{}{
		"context": base64.StdEncoding.EncodeToString(contextStream.Bytes()),
	}
	s, err := v.client.Logical().Write(v.genDataKeyEndpoint(keyID), payload)

	if err != nil {
		return key, sealedKey, err
	}
	sealKey := s.Data["ciphertext"].(string)
	plainKey, err := base64.StdEncoding.DecodeString(s.Data["plaintext"].(string))
	if err != nil {
		return key, sealedKey, err
	}
	copy(key[:], []byte(plainKey))
	return key, []byte(sealKey), nil
}

// unsealKMSKey unseals the sealedKey using the Vault master key
// referenced by the keyID. The plain text key is returned on success.
func (v *vaultService) UnsealKey(keyID string, sealedKey []byte, ctx crypto.Context) (key [32]byte, err error) {
	contextStream := new(bytes.Buffer)
	ctx.WriteTo(contextStream)
	payload := map[string]interface{}{
		"ciphertext": string(sealedKey),
		"context":    base64.StdEncoding.EncodeToString(contextStream.Bytes()),
	}
	s, err := v.client.Logical().Write(v.decryptEndpoint(keyID), payload)
	if err != nil {
		return key, err
	}
	base64Key := s.Data["plaintext"].(string)
	plainKey, err1 := base64.StdEncoding.DecodeString(base64Key)
	if err1 != nil {
		return key, err
	}
	copy(key[:], []byte(plainKey))

	return key, nil
}

func (v *vaultService) GetKeyID() string {
	return v.config.Key.Name
}
