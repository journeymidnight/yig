package _go

import (
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/journeymidnight/aws-sdk-go/aws"
	"github.com/journeymidnight/aws-sdk-go/service/s3"
	. "github.com/journeymidnight/yig/test/go/lib"
)

func Test_BucketWebSite(t *testing.T) {
	sc := NewS3()
	defer sc.CleanEnv()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	err = sc.PutBucketWebsite(TEST_BUCKET, "index.html", "error.html")
	if err != nil {
		t.Fatal("PutBucketWebsite err:", err)
		panic(err)
	}
	out, err := sc.GetBucketWebsite(TEST_BUCKET)
	t.Log("Webstite:", out)

	err = sc.DeleteBucketWebsite(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucketWebsite err:", err)
		panic(err)
	}
}

const (
	testIndexHTML = `<html><body>website index</body></html>`
	testErrorHTML = `<html><body>website error</body></html>`
)

type WebsiteTestUnit struct {
	WebsiteConfiguration *s3.WebsiteConfiguration
	Buckets              []string
	Objects              []ObjectInput
	Cases                []Case
	Fn                   func(t *testing.T, input string) (code int, output string)
}

type ObjectInput struct {
	Bucket string
	Key    string
	Value  string
}

type Case struct {
	Input              string
	ExpectedStatusCode int
	ExpectedContent    string
	IsRedirect         bool
}

func doGet(t *testing.T, input string) (code int, output string) {
	res, err := http.Get(input)
	if err != nil {
		t.Error("http get", input, "failed:", err)
		return 0, ""
	}
	defer res.Body.Close()
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Error("Read data from", input, "failed:", err)
		return 0, ""
	}
	return res.StatusCode, string(data)
}

var testUnits = []WebsiteTestUnit{
	// The configuration in the request specifies index.html as the index document. It also specifies the optional error document, error.html.
	{
		WebsiteConfiguration: &s3.WebsiteConfiguration{
			IndexDocument: &s3.IndexDocument{Suffix: aws.String("index.html")},
			ErrorDocument: &s3.ErrorDocument{Key: aws.String("error.html")},
		},
		Buckets: []string{TEST_BUCKET},
		Objects: []ObjectInput{
			{TEST_BUCKET, "index.html", testIndexHTML},
			{TEST_BUCKET, "error.html", testErrorHTML},
		},
		Fn: doGet,
		Cases: []Case{
			{"http://" + TEST_BUCKET + "." + Endpoint, 200, testIndexHTML, false},
			{"http://" + TEST_BUCKET + "." + Endpoint + "/aaa.txt", 404, testErrorHTML, false},
		},
	},
	// Configure bucket as a website but redirect all requests
	{
		WebsiteConfiguration: &s3.WebsiteConfiguration{
			RedirectAllRequestsTo: &s3.RedirectAllRequestsTo{HostName: aws.String("baidu.com")},
		},
		Buckets: []string{TEST_BUCKET},
		Objects: []ObjectInput{
			{TEST_BUCKET, "index.html", testIndexHTML},
			{TEST_BUCKET, "error.html", testErrorHTML},
		},
		Fn: doGet,
		Cases: []Case{
			{"http://" + TEST_BUCKET + "." + Endpoint + "/index.html", 200, testIndexHTML, true},
		},
	},
	// Configure bucket as a website and also specify optional redirection rules
	{
		WebsiteConfiguration: &s3.WebsiteConfiguration{
			IndexDocument: &s3.IndexDocument{Suffix: aws.String("index.html")},
			ErrorDocument: &s3.ErrorDocument{Key: aws.String("error.html")},
			RoutingRules: []*s3.RoutingRule{
				{
					Condition: &s3.Condition{KeyPrefixEquals: aws.String("docs/")},
					Redirect:  &s3.Redirect{ReplaceKeyPrefixWith: aws.String("documents/")},
				},
			},
		},
		Buckets: []string{TEST_BUCKET},
		Objects: []ObjectInput{
			{TEST_BUCKET, "documents/index.html", testIndexHTML},
		},
		Fn: doGet,
		Cases: []Case{
			{"http://" + TEST_BUCKET + "." + Endpoint + "/documents/", 200, testIndexHTML, false},
			{"http://" + TEST_BUCKET + "." + Endpoint + "/docs/", 200, testIndexHTML, false},
		},
	},
	// Configure bucket as a website and redirect errors
	// TODO: Find a redirect host can be used.
	//{
	//	WebsiteConfiguration: &s3.WebsiteConfiguration{
	//		IndexDocument: &s3.IndexDocument{Suffix: aws.String("index.html")},
	//		ErrorDocument: &s3.ErrorDocument{Key: aws.String("error.html")},
	//		RoutingRules: []*s3.RoutingRule{
	//			{
	//				Condition: &s3.Condition{HttpErrorCodeReturnedEquals: aws.String("404")},
	//				Redirect: &s3.Redirect{
	//					HostName:             aws.String("s3.test"),
	//					ReplaceKeyPrefixWith: aws.String("documents/"),
	//				},
	//			},
	//		},
	//	},
	//},
	//{
	// Configure a bucket as a website and redirect folder requests to a page
	//	WebsiteConfiguration: &s3.WebsiteConfiguration{
	//		IndexDocument: &s3.IndexDocument{Suffix: aws.String("index.html")},
	//		ErrorDocument: &s3.ErrorDocument{Key: aws.String("error.html")},
	//		RoutingRules: []*s3.RoutingRule{
	//			{
	//				Condition: &s3.Condition{KeyPrefixEquals: aws.String("docs/")},
	//				Redirect:  &s3.Redirect{ReplaceKeyWith: aws.String("error.html")},
	//			},
	//		},
	//	},
	//}
}

func Test_BucketWebsiteCases(t *testing.T) {
	sc := NewS3()
	CleanUnits(sc)
	for _, unit := range testUnits {
		for _, b := range unit.Buckets {
			err := sc.MakeBucket(b)
			if err != nil {
				t.Fatal("MakeBucket err:", err)
			}
			err = sc.PutBucketWebsiteWithConf(b, unit.WebsiteConfiguration)
			if err != nil {
				t.Fatal("PutBucketWebsiteWithConf err:", err)
			}
		}
		for _, o := range unit.Objects {
			err := sc.PutObject(o.Bucket, o.Key, o.Value)
			if err != nil {
				t.Fatal("PutObject err:", err)
			}
			err = sc.PutObjectAcl(o.Bucket, o.Key, ObjectCannedACLPublicRead)
			if err != nil {
				t.Fatal("PutObjectAcl err:", err)
			}
		}
		for _, c := range unit.Cases {
			code, out := unit.Fn(t, c.Input)
			if code != c.ExpectedStatusCode {
				t.Fatal("Test case failed.", "Input:", c.Input, "Code:", code, "ExpectedStatusCode:", c.ExpectedStatusCode)
			}
			if !c.IsRedirect && out != c.ExpectedContent {
				t.Fatal("Test case failed.", "Input:", c.Input, "Output:", out, "ExpectedContent:", c.ExpectedContent)
			}
		}
		clean(sc, unit)
	}
}

func CleanUnits(sc *S3Client) {
	for _, unit := range testUnits {
		clean(sc, unit)
	}
}

func clean(sc *S3Client, unit WebsiteTestUnit) {
	for _, o := range unit.Objects {
		sc.DeleteObject(o.Bucket, o.Key)
	}
	for _, b := range unit.Buckets {
		sc.DeleteBucket(b)
	}
}
