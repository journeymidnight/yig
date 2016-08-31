package helper

import (
	"reflect"
)

// mimic `?:` operator
// Need type assertion to convert output to expected type
func Ternary(IF bool, THEN interface{}, ELSE interface{}) interface{} {
	if IF {
		return THEN
	} else {
		return ELSE
	}
}

// Get keys of a map, i.e.
// map[string]interface{} -> []string
// Note that some type checks are omitted for efficiency, you need to ensure them yourself,
// otherwise your program should panic
func Keys(v interface{}) []string {
	rv := reflect.ValueOf(v)
	result := make([]string, 0, rv.Len())
	for _, kv := range rv.MapKeys() {
		result = append(result, kv.String())
	}
	return result, nil
}
