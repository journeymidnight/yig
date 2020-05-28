package types

type UserQos struct {
	UserID    string
	ReadQps   int
	WriteQps  int
	Bandwidth int // in KiBps
}
