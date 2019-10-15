package tidbclient

import (
	"testing"

	"github.com/magiconair/properties/assert"
)

func Test_GetPlaceHolders(t *testing.T) {
	p1 := getPlaceHolders(0, 0)
	assert.Equal(t, p1, "")

	p2 := getPlaceHolders(3, 1)
	assert.Equal(t, p2, "?,?,?")

	p3 := getPlaceHolders(3, 2)
	assert.Equal(t, p3, "(?,?),(?,?),(?,?)")
}
