package types

type UserQos struct {
	UserID    string
	Qps       int
	Bandwidth int // in KiBps
}
