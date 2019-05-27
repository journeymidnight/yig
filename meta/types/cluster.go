package types

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/journeymidnight/yig/helper"
)

type Cluster struct {
	Fsid   string
	Pool   string
	Weight int
}

func (c *Cluster) Serialize() (map[string]interface{}, error) {
	fields := make(map[string]interface{})
	bytes, err := helper.MsgPackMarshal(c)
	if err != nil {
		return nil, err
	}
	fields[FIELD_NAME_BODY] = string(bytes)
	return fields, nil
}

func (c *Cluster) Deserialize(fields map[string]string) (interface{}, error) {
	body, ok := fields[FIELD_NAME_BODY]
	if !ok {
		return nil, errors.New(fmt.Sprintf("no field %s found", FIELD_NAME_BODY))
	}

	err := helper.MsgPackUnMarshal([]byte(body), c)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Cluster) GetValues() (values map[string]map[string][]byte, err error) {
	values = map[string]map[string][]byte{
		CLUSTER_COLUMN_FAMILY: map[string][]byte{
			"weight": []byte(strconv.Itoa(c.Weight)),
		},
	}
	return
}
