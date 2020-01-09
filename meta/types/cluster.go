package types

type BackendType uint8

const (
	BackendCeph BackendType = iota
)

type Cluster struct {
	Backend BackendType
	Fsid    string
	Pool    string
	Weight  int
}
