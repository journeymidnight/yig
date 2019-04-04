package _go

import (
	"bufio"
	. "github.com/journeymidnight/yig/test/go/lib"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func Test_Logging_MakeBucket(t *testing.T) {
	sc := NewS3()
	err := sc.MakeBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("MakeBucket err:", err)
		panic(err)
	}
	t.Log("MakeBucket Success.")

	file, err := os.Open("../../access.log")
	defer file.Close()
	if err != nil {
		t.Fatal("Failed to read access log file ", err)
	}

	//local log
	time_local := time.Now().Format("2006-01-02 15:04:05")
	makeBucketLog := "[" + time_local + "] PUT / HTTP/1.1 mybucket.s3.test.com:8080 mybucket 10.5.0.1:56024" +
		"  - 145 - - 42 200 0  \"aws-sdk-go/1.17.4 (go1.12; linux; amd64)\" true cn-bj-1"
	makeBucketLogSplit := strings.Split(makeBucketLog, " ")

	//access log
	lastLog := getLastLog(file)
	lastLogSplit := strings.Split(string(lastLog), " ")

	if len(makeBucketLogSplit) == len(lastLogSplit) {
		for i := 0; i < len(makeBucketLogSplit); i++ {
			if i < 2 {
				//time test
				//get time string from log
				time_local := logToTimestr(makeBucketLogSplit[0], makeBucketLogSplit[1])
				time_log := logToTimestr(lastLogSplit[0], lastLogSplit[1])
				//string to stamp
				timestamp_local := stringToTimestamp(time_local)
				timestamp_log := stringToTimestamp(time_log)

				if timestamp_local >= (timestamp_log + 1000) {
					t.Fatal("time inconformity", timestamp_local, timestamp_log)
				}
			} else if i == 7 {
				//http_x_real_ip test
				if len(makeBucketLogSplit) != len(lastLogSplit) {
					t.Fatal("http_x_real_ip inconformity:", makeBucketLogSplit[i], lastLogSplit[i], i)
				}
			} else if i == 13 {
				//request_time test
				continue
			} else if strings.Compare(makeBucketLogSplit[i], lastLogSplit[i]) != 0 {
				t.Log(makeBucketLog)
				t.Log(lastLog)
				t.Fatal("other inconformity:", makeBucketLogSplit[i], lastLogSplit[i], i)
			}
		}
	} else {
		t.Fatal("log length inconformity:", len(makeBucketLogSplit), len(lastLogSplit))
	}
}

func Test_Logging_GetBucketAcl(t *testing.T) {
	sc := NewS3()
	_, err := sc.GetBucketAcl(TEST_BUCKET)
	if err != nil {
		t.Fatal("GetBucketAcl err:", err)
	}
	t.Log("GetBucketAcl Success!")

	file, err := os.Open("../../access.log")
	defer file.Close()
	if err != nil {
		t.Fatal("Failed to read access log file ", err)
	}

	//local log
	time_local := time.Now().Format("2006-01-02 15:04:05")
	getBucketAclLog := "[" + time_local + "] GET /?acl= HTTP/1.1 mybucket.s3.test.com:8080 mybucket 10.5.0.1:33754" +
		"  hehehehe 0 - - 5 200 438  \"aws-sdk-go/1.17.4 (go1.12; linux; amd64)\" true cn-bj-1"
	getBucketAclLogSplit := strings.Split(getBucketAclLog, " ")

	//access log
	lastLog := getLastLog(file)
	lastLogSplit := strings.Split(lastLog, " ")

	if len(getBucketAclLogSplit) == len(lastLogSplit) {
		for i := 0; i < len(getBucketAclLogSplit); i++ {
			if i < 2 {
				//time test
				//get time string from log
				time_local := logToTimestr(getBucketAclLogSplit[0], getBucketAclLogSplit[1])
				time_log := logToTimestr(lastLogSplit[0], lastLogSplit[1])
				//string to stamp
				timestamp_local := stringToTimestamp(time_local)
				timestamp_log := stringToTimestamp(time_log)

				if timestamp_local >= (timestamp_log + 1000) {
					t.Fatal("time inconformity", timestamp_local, timestamp_log)
				}
			} else if i == 7 {
				//http_x_real_ip test
				if len(getBucketAclLogSplit) != len(lastLogSplit) {
					t.Fatal("http_x_real_ip inconformity:", getBucketAclLogSplit[i], lastLogSplit[i], i)
				}
			} else if i == 13 {
				//request_time test
				continue
			} else if strings.Compare(getBucketAclLogSplit[i], lastLogSplit[i]) != 0 {
				t.Log(getBucketAclLog)
				t.Log(string(lastLog))
				t.Fatal("other inconformity:", getBucketAclLogSplit[i], lastLogSplit[i], i)
			}
		}
	} else {
		t.Fatal("log length inconformity:", len(getBucketAclLogSplit), len(lastLogSplit))
	}

}

func Test_Logging_DeleteBucket(t *testing.T) {
	sc := NewS3()
	err := sc.DeleteBucket(TEST_BUCKET)
	if err != nil {
		t.Fatal("DeleteBucket err:", err)
		panic(err)
	}
	t.Log("DeleteBucket Success.")

	file, err := os.Open("../../access.log")
	defer file.Close()
	if err != nil {
		t.Fatal("Failed to read access log file ", err)
	}

	//local log
	time_local := time.Now().Format("2006-01-02 15:04:05")
	deleteBucketLog := "[" + time_local + "] DELETE / HTTP/1.1 mybucket.s3.test.com:8080 mybucket 10.5.0.1:54172" +
		"  hehehehe 0 - - 36 200 0  \"aws-sdk-go/1.17.4 (go1.12; linux; amd64)\" true cn-bj-1"
	deleteBucketLogSplit := strings.Split(deleteBucketLog, " ")

	//access log
	lastLog := getLastLog(file)
	lastLogSplit := strings.Split(lastLog, " ")

	if len(deleteBucketLogSplit) == len(lastLogSplit) {
		for i := 0; i < len(deleteBucketLogSplit); i++ {
			if i < 2 {
				//time test
				//get time string from log
				time_local := logToTimestr(deleteBucketLogSplit[0], deleteBucketLogSplit[1])
				time_log := logToTimestr(lastLogSplit[0], lastLogSplit[1])
				//string to stamp
				timestamp_local := stringToTimestamp(time_local)
				timestamp_log := stringToTimestamp(time_log)

				if timestamp_local >= (timestamp_log + 1000) {
					t.Fatal("time inconformity", timestamp_local, timestamp_log)
				}
			} else if i == 7 {
				//http_x_real_ip test
				if len(deleteBucketLogSplit) != len(lastLogSplit) {
					t.Fatal("http_x_real_ip inconformity:", deleteBucketLogSplit[i], lastLogSplit[i], i)
				}
			} else if i == 13 {
				//request_time test
				continue
			} else if strings.Compare(deleteBucketLogSplit[i], lastLogSplit[i]) != 0 {
				t.Log(deleteBucketLog)
				t.Log(lastLog)
				t.Fatal("other inconformity:", deleteBucketLogSplit[i], lastLogSplit[i], i)
			}
		}
	} else {
		t.Fatal("log length inconformity:", len(deleteBucketLogSplit), len(lastLogSplit))
	}

}

func logToTimestr(part1, part2 string) string {
	time_str := part1 + " " + part2
	strs := []rune(time_str)
	time_str = string(strs[1 : len(strs)-1])
	return time_str
}

func stringToTimestamp(s string) int64 {
	loc, _ := time.LoadLocation("Local")
	theTime, _ := time.ParseInLocation("2006-01-02 15:04:05", s, loc)
	timeStamp := theTime.Unix()
	return timeStamp
}

func getLastLog(file *os.File) string {
	buf := bufio.NewReader(file)
	var lastLog string
	for {
		a, _, c := buf.ReadLine()
		if c == io.EOF {
			break
		}
		//get last column of access log
		lastLog = string(a)
	}
	return lastLog
}
