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
	err := sc.MakeBucket(TestBucket)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	err = sc.PutBucketWebsite(TestBucket, "index.html", "error.html")
	if err != nil {
		t.Fatal("PutBucketWebsite err:", err)
		panic(err)
	}
	out, err := sc.GetBucketWebsite(TestBucket)
	t.Log("Webstite:", out)

	err = sc.DeleteBucketWebsite(TestBucket)
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
		Buckets: []string{TestBucket},
		Objects: []ObjectInput{
			{TestBucket, "index.html", testIndexHTML},
			{TestBucket, "error.html", testErrorHTML},
		},
		Fn: doGet,
		Cases: []Case{
			{"http://" + TestBucket + "." + Endpoint, 200, testIndexHTML, false},
			{"http://" + TestBucket + "." + Endpoint + "/aaa.txt", 404, testErrorHTML, false},
		},
	},
	// Configure bucket as a website but redirect all requests
	{
		WebsiteConfiguration: &s3.WebsiteConfiguration{
			RedirectAllRequestsTo: &s3.RedirectAllRequestsTo{HostName: aws.String("sina.com")},
		},
		Buckets: []string{TestBucket},
		Objects: []ObjectInput{
			{TestBucket, "index.html", testIndexHTML},
			{TestBucket, "error.html", testErrorHTML},
		},
		Fn: doGet,
		Cases: []Case{
			{"http://" + TestBucket + "." + Endpoint + "/index.html", 200, testIndexHTML, true},
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
		Buckets: []string{TestBucket},
		Objects: []ObjectInput{
			{TestBucket, "documents/", ""},
			{TestBucket, "documents/index.html", testIndexHTML},
		},
		Fn: doGet,
		Cases: []Case{
			{"http://" + TestBucket + "." + Endpoint + "/documents/", 200, testIndexHTML, false},
			{"http://" + TestBucket + "." + Endpoint + "/docs/", 200, testIndexHTML, false},
		},
	},
	// Configure bucket as a website and redirect errors
	{
		WebsiteConfiguration: &s3.WebsiteConfiguration{
			IndexDocument: &s3.IndexDocument{Suffix: aws.String("index.html")},
			ErrorDocument: &s3.ErrorDocument{Key: aws.String("error.html")},
			RoutingRules: []*s3.RoutingRule{
				{
					Condition: &s3.Condition{HttpErrorCodeReturnedEquals: aws.String("404")},
					Redirect: &s3.Redirect{
						HostName:             aws.String(TestBucket + ".s3-internal.test.com:8080"),
						ReplaceKeyPrefixWith: aws.String("docs/"),
					},
				},
			},
		},
		Buckets: []string{TestBucket},
		Objects: []ObjectInput{
			{TestBucket, "docs/", ""},
			{TestBucket, "docs/error.html", testErrorHTML},
		},
		Fn: doGet,
		Cases: []Case{
			{"http://" + TestBucket + "." + Endpoint + "/error.html", 200, testErrorHTML, true},
		},
	},
	//  Configure a bucket as a website and redirect folder requests to a page
	{
		WebsiteConfiguration: &s3.WebsiteConfiguration{
			IndexDocument: &s3.IndexDocument{Suffix: aws.String("index.html")},
			ErrorDocument: &s3.ErrorDocument{Key: aws.String("error.html")},
			RoutingRules: []*s3.RoutingRule{
				{
					Condition: &s3.Condition{KeyPrefixEquals: aws.String("docs/")},
					Redirect:  &s3.Redirect{ReplaceKeyWith: aws.String("errorPage")},
				},
			},
		},

		Buckets: []string{TestBucket},
		Objects: []ObjectInput{
			{TestBucket, "docs/", ""},
			{TestBucket, "docs/test", TestValue},
			{TestBucket, "errorPage", testErrorHTML},
		},
		Fn: doGet,
		Cases: []Case{
			{"http://" + TestBucket + "." + Endpoint + "/docs/test", 200, testErrorHTML, true},
		},
	},
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
