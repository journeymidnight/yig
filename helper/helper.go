package helper

// mimic `?:` operator
// Need type assertion to convert output to expected type
func Ternary(IF bool, THEN interface{}, ELSE interface{}) interface{} {
	if IF {
		return THEN
	} else {
		return ELSE
	}
}

