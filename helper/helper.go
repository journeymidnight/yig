package helper

// mimic `?:` operator
func Ternary(IF bool, THEN interface{}, ELSE interface{})  interface{} {
	if IF {
		return THEN
	} else {
		return ELSE
	}
}
