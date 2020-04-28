package types

type LifeCycle struct {
	BucketName string
	Status     string // status of this entry, in Pending/Deleting
}

type ScanLifeCycleResult struct {
	Truncated  bool
	NextMarker string
	// List of LifeCycles info for this request.
	Lcs []LifeCycle
}
