package types

import (
	"strconv"
)

type Cluster struct {
	Fsid   string
	Pool   string
	Weight int
}

func (c Cluster) GetValues() (values map[string]map[string][]byte, err error) {
	values = map[string]map[string][]byte{
		CLUSTER_COLUMN_FAMILY: map[string][]byte{
			"weight": []byte(strconv.Itoa(c.Weight)),
		},
	}
	return
}
