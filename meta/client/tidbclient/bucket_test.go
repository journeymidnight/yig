package tidbclient_test

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/meta/client/tidbclient"
	"github.com/stretchr/testify/assert"
	"testing"
)

var bucketColumns = []string{
	"bucketname", "acl", "cors", "lc",
	"uid", "policy", "website", "encryption",
	"createtime", "usages", "versioning",
}

var bucketColumnValues = []driver.Value{
	"hehe", []byte("{}"), []byte("{}"), []byte("{}"),
	"haha", []byte("{}"), []byte("{}"),
	"2010-04-01 00:00:00", 4, []byte("{}"),
}

func newClient() (client *tidbclient.TidbClient, mock sqlmock.Sqlmock, err error) {
	var db *sql.DB
	db, mock, err = sqlmock.New()
	if err != nil {
		return
	}
	client = &tidbclient.TidbClient{db}
	return
}

func TestTidbClient_GetBucket(t *testing.T) {
	client, mock, err := newClient()
	if err != nil {
		t.Error("Error creating mock client:", err)
	}
	defer client.Client.Close()

	// Query 1
	mock.ExpectQuery("select \\* from buckets where bucketname=(.+)").
		WithArgs("hehe").
		WillReturnRows(
			sqlmock.NewRows(bucketColumns).
				AddRow(bucketColumnValues...),
		)
	// Query 2
	mock.ExpectQuery("select \\* from buckets where bucketname=(.+)").
		WithArgs("haha").WillReturnError(sql.ErrNoRows)
	// Query 3
	someOtherError := errors.New("some other error")
	mock.ExpectQuery("select \\* from buckets where bucketname=(.+)").
		WithArgs("hoho").WillReturnError(someOtherError)
	// Query 4
	badJson := bucketColumnValues
	badJson[3] = ""
	mock.ExpectQuery("select \\* from buckets where bucketname=(.+)").
		WithArgs("hhhh").
		WillReturnRows(
			sqlmock.NewRows(bucketColumns).
				AddRow(badJson...),
		)

	// Query 1
	bucket, err := client.GetBucket("hehe")
	assert.Nil(t, err)
	assert.Equal(t, "haha", bucket.OwnerId)
	assert.Equal(t, datatype.Acl{}, bucket.ACL)
	// Query 2
	_, err = client.GetBucket("haha")
	assert.Equal(t, ErrNoSuchBucket, err)
	// Query 3
	_, err = client.GetBucket("hoho")
	assert.Equal(t, someOtherError, err)
	// Query 4
	_, err = client.GetBucket("hhhh")
	assert.NotNil(t, err)
}
