package xxtea

import (
	"encoding/base64"
	"fmt"
	"testing"
)

func Test_XXTEA(t *testing.T) {
	str := "Hello World! 你好，中国！asdaczvhgjzxc!@#$%^&*()_+[]{}|:<>?;',./"
	key := "1234567890"
	encrypt_data := Encrypt([]byte(str), []byte(key))
	fmt.Println(base64.StdEncoding.EncodeToString(encrypt_data))

	//Test format between 
	url_str := EncryptStdToURLString(str, key)
	fmt.Println(url_str)
	std_str, _ := DecryptURLToStdString(url_str, key)
	fmt.Println(std_str)

	decrypt_data := string(Decrypt(encrypt_data, []byte(key)))
	if str != decrypt_data {
		t.Error(str)
		t.Error(decrypt_data)
		t.Error("fail!")
	}

	if std_str != str {}

}
