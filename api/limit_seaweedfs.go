// +build seaweedfs

package api

const (
	// maximum object size per PUT request is 30MB, limit introduced by cannyls
	maxObjectSize = 1024 * 1024 * 30
	// minimum Part size for multipart upload is 5MB
	minPartSize = 1024 * 1024 * 5
	// maximum Part ID for multipart upload is 100000 (Acceptable values range from 1 to 100000 inclusive)
	// increase maxPartID to 100000 so as to keep max Object size to 3TB, changed because of maxObjectSize
	maxPartID = 100000
)