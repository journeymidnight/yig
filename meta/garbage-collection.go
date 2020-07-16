package meta

import (
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
)

// Generate garbage object and send to Kafka
func (m *Meta) PutObjectToGarbageCollection(object *Object) error {
	garbageObject := GetGcInfoFromObject(object)
	v, err := helper.MsgPackMarshal(garbageObject)
	if err != nil {
		return err
	}
	m.GarbageCollectionProducer.Publish("", v)
	return nil
}

func (m *Meta) PutFreezerToGarbageCollection(freezer *Freezer) error {
	object := freezer.ToObject()
	return m.PutObjectToGarbageCollection(&object)
}
