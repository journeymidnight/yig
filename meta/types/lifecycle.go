package types

const (
	LcEnding  = "Ending"
	LcPending = "Pending"
)

type LifeCycle struct {
	BucketName string
	Status     string // status of this entry, in Pending/Ending
	StartTime  uint64 // Timestamp(nanosecond)
	EndTime    uint64 // Timestamp(nanosecond)
}

type ScanLifeCycleResult struct {
	Truncated  bool
	NextMarker string
	// List of LifeCycles info for this request.
	Lcs []LifeCycle
}

func (l LifeCycle) GetCreateSql() (string, []interface{}) {
	sql := "insert into lifecycle(bucketname,status,starttime,endtime) values (?,?,?,?);"
	args := []interface{}{l.BucketName, l.Status, l.StartTime, l.EndTime}
	return sql, args
}

func (l LifeCycle) GetUpdateSql() (string, []interface{}) {
	sql := "update buckets set status=?,starttime=?,endtime=? where bucketname=?"
	args := []interface{}{l.Status, l.StartTime, l.EndTime, l.BucketName}
	return sql, args
}
