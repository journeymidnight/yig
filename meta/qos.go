package meta

import "github.com/journeymidnight/yig/meta/types"

// Solely used for building in-memory cache, so not bother caching in Redis again
func (m *Meta) GetAllUserQos() (userQos map[string]types.UserQos, err error) {
	return m.Client.GetAllUserQos()
}
