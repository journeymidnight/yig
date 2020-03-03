package meta

import . "github.com/journeymidnight/yig/meta/types"

// Insert object to `garbageCollection` table
func (m *Meta) PutObjectToGarbageCollection(object *Object) error {
	return m.Client.PutObjectToGarbageCollection(object, nil)
}

func (m *Meta) ScanGarbageCollection(limit int) ([]GarbageCollection, error) {
	return m.Client.ScanGarbageCollection(limit)
}

func (m *Meta) RemoveGarbageCollection(garbage GarbageCollection) error {
	return m.Client.RemoveGarbageCollection(garbage)
}
