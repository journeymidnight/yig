package types

type SimpleIndex struct {
	Index []int64
}

func (array *SimpleIndex) SearchLowerBound(key int64) int {
	var low int = 0
	var high int = len(array.Index) - 1
	var mid = (low + high) / 2

	if array.Index[low] > key {
		return -1
	}

	for low <= high {

		if array.Index[mid] == key {
			break
		}

		if array.Index[mid] > key {
			high = mid - 1
		} else {
			low = mid + 1
		}
		mid = (low + high) / 2

	}

	return mid

}

func (array *SimpleIndex) SearchUpperBound(key int64) int {
	var low int = 0
	var high int = len(array.Index) - 1
	var mid = (low + high) / 2

	if array.Index[high] <= key {
		return -1
	}

	for low <= high {
		if array.Index[mid] > key {
			if mid-1 >= low && key >= array.Index[mid-1] {
				return mid
			} else {
				high = mid - 1
			}
		} else {
			if mid+1 <= high && key < array.Index[mid+1] {
				return mid + 1
			} else {
				low = mid + 1
			}
		}
		mid = (low + high) / 2
	}

	return mid
}
