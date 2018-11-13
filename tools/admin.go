package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"io/ioutil"
	"net/http"
	"os"
)

var client = &http.Client{}

type Config struct {
	RequestUrl string
	AdminKey   string
}

var config Config

func printHelp() {
	fmt.Println("Usage: admin <commands> [options...] ")
	fmt.Println("Commands: usage|bucket|object|user|cachehit")
	fmt.Println("Options:")
	fmt.Println(" -b, --bucket   Specify bucket to operate")
	fmt.Println(" -u, --uid      Specify user name to operate")
	fmt.Println(" -o, --object   Specify object to operate")
}

func isParaEmpty(p string) bool {
	if p == "" {
		fmt.Printf("Bad usage, Try admin")
		return true
	} else {
		return false
	}
}

func getusage(bucket string) {
	if isParaEmpty(bucket) {
		return
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"bucket": bucket,
	})

	tokenString, err := token.SignedString([]byte(config.AdminKey))

	if err == nil {
		//go use token
		fmt.Printf("\nHS256 = %v\n", tokenString)
	} else {
		fmt.Println("internal error", err)
		return
	}

	url := config.RequestUrl + "/admin/usage"
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Println("create request failed", err)
		return
	}
	request.Header.Set("Authorization", "Bearer "+tokenString)
	response, err := client.Do(request)
	if err != nil {
		fmt.Println("send request failed", err)
		return
	}
	if response.StatusCode != 200 {
		fmt.Println("getBucketInfo failed as status != 200", response.StatusCode)
		return
	}

	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	fmt.Println(string(body))
}

func getBucketInfo(bucket string) {
	if isParaEmpty(bucket) {
		return
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"bucket": bucket,
	})

	tokenString, err := token.SignedString([]byte(config.AdminKey))

	if err == nil {
		//go use token
		fmt.Printf("\nHS256 = %v\n", tokenString)
	} else {
		fmt.Println("internal error", err)
		return
	}

	url := config.RequestUrl + "/admin/bucket"
	request, _ := http.NewRequest("GET", url, nil)
	request.Header.Set("Authorization", "Bearer "+tokenString)
	response, _ := client.Do(request)
	if response.StatusCode != 200 {
		fmt.Println("getBucketInfo failed as status != 200", response.StatusCode)
		return
	}

	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	fmt.Println(string(body))

}

func getUserInfo(uid string) {
	if isParaEmpty(uid) {
		return
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"uid": uid,
	})

	tokenString, err := token.SignedString([]byte(config.AdminKey))

	if err == nil {
		//go use token
		fmt.Printf("\nHS256 = %v\n", tokenString)
	} else {
		fmt.Println("internal error", err)
		return
	}

	url := config.RequestUrl + "/admin/user"
	request, _ := http.NewRequest("GET", url, nil)
	request.Header.Set("Authorization", "Bearer "+tokenString)
	response, _ := client.Do(request)
	if response.StatusCode != 200 {
		fmt.Println("getBucketInfo failed as status != 200", response.StatusCode)
		return
	}

	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	fmt.Println(string(body))

}

func getObjectInfo(bucket string, object string) {
	if isParaEmpty(bucket) || isParaEmpty(object) {
		return
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"bucket": bucket,
		"object": object,
	})

	tokenString, err := token.SignedString([]byte(config.AdminKey))

	if err == nil {
		//go use token
		fmt.Printf("\nHS256 = %v\n", tokenString)
	} else {
		fmt.Println("internal error", err)
		return
	}

	url := config.RequestUrl + "/admin/object"
	request, _ := http.NewRequest("GET", url, nil)
	request.Header.Set("Authorization", "Bearer "+tokenString)
	response, _ := client.Do(request)
	if response.StatusCode != 200 {
		fmt.Println("getBucketInfo failed as status != 200", response.StatusCode)
		return
	}

	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	fmt.Println(string(body))
}

func getCacheHit() {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{})

	tokenString, err := token.SignedString([]byte(config.AdminKey))

	if err == nil {
		//go use token
		fmt.Printf("\nHS256 = %v\n", tokenString)
	} else {
		fmt.Println("internal error", err)
		return
	}

	url := config.RequestUrl + "/admin/cachehit"
	request, _ := http.NewRequest("GET", url, nil)
	request.Header.Set("Authorization", "Bearer "+tokenString)
	response, _ := client.Do(request)
	if response.StatusCode != 200 {
		fmt.Println("getBucketInfo failed as status != 200", response.StatusCode)
		return
	}

	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	fmt.Println(string(body))

}

func main() {
	f, err := os.Open("./admin.json")
	if err != nil {
		panic("Cannot open admin.json")
	}
	defer f.Close()

	err = json.NewDecoder(f).Decode(&config)
	if err != nil {
		panic("Failed to parse yig.json: " + err.Error())
	}
	if len(os.Args) <= 1 {
		printHelp()
		return
	}
	mySet := flag.NewFlagSet("", flag.ExitOnError)
	bucket := mySet.String("b", "", "bucket name")
	uid := mySet.String("u", "", "user name")
	object := mySet.String("o", "", "object name")
	mySet.Parse(os.Args[2:])
	fmt.Println("command:", os.Args[1], "bucket:", *bucket, "user:", *uid, "object:", *object)
	switch os.Args[1] {
	case "usage":
		getusage(*bucket)
	case "bucket":
		getBucketInfo(*bucket)
	case "user":
		getUserInfo(*uid)
	case "object":
		getObjectInfo(*bucket, *object)
	case "cachehit":
		getCacheHit()
	default:
		printHelp()
		return
	}
}
