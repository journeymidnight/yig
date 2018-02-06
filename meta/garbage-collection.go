package meta

import . "github.com/journeymidnight/yig/meta/types"

// Insert object to `garbageCollection` table
func (m *Meta) PutObjectToGarbageCollection(object *Object) error {
	return m.Client.PutObjectToGarbageCollection(object)
}

func (m *Meta) ScanGarbageCollection(limit int, startRowKey string) ([]GarbageCollection, error) {
	return m.Client.ScanGarbageCollection(limit, startRowKey)
}

func (m *Meta) RemoveGarbageCollection(garbage GarbageCollection) error {
	return m.Client.RemoveGarbageCollection(garbage)
}
