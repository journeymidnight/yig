package helper

func Filter(xs []string, f func(string) bool) []string {
	var ans []string
	for _, x := range xs {
		if f(x) {
			ans = append(ans, x)
		}
	}
	return ans
}

func Map(xs []string, f func(string) string) []string {
	var ans []string
	for _, x := range xs {
		ans = append(ans, f(x))
	}
	return ans
}
