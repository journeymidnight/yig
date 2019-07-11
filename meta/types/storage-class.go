package types


import . "github.com/journeymidnight/yig/error"


type StorageClass uint8

// Reference：https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/dev/storage-class-intro.html
const (
	// ObjectStorageClassStandard is a ObjectStorageClass enum value
	ObjectStorageClassStandard StorageClass = iota

	// ObjectStorageClassReducedRedundancy is a ObjectStorageClass enum value
	ObjectStorageClassReducedRedundancy

	// ObjectStorageClassGlacier is a ObjectStorageClass enum value
	ObjectStorageClassGlacier

	// ObjectStorageClassStandardIa is a ObjectStorageClass enum value
	ObjectStorageClassStandardIa

	// ObjectStorageClassOnezoneIa is a ObjectStorageClass enum value
	ObjectStorageClassOnezoneIa

	// ObjectStorageClassIntelligentTiering is a ObjectStorageClass enum value
	ObjectStorageClassIntelligentTiering

	// ObjectStorageClassIntelligentTiering is a ObjectStorageClass enum value
	ObjectStorageClassDeepArchive
)

var (
	StorageClassIndexMap = map[StorageClass]string{
		ObjectStorageClassStandard:           "STANDARD",
		ObjectStorageClassStandardIa:         "STANDARD_IA",
		ObjectStorageClassIntelligentTiering: "INTELLIGENT_TIERING",
		ObjectStorageClassOnezoneIa:          "ONEZONE_IA",
		ObjectStorageClassGlacier:            "GLACIER",
		ObjectStorageClassDeepArchive:        "DEEP_ARCHIVE",
		ObjectStorageClassReducedRedundancy:  "RRS",
	}

	StorageClassStringMap = map[string]StorageClass{
		"STANDARD":            ObjectStorageClassStandard,
		"STANDARD_IA":         ObjectStorageClassStandardIa,
		"INTELLIGENT_TIERING": ObjectStorageClassIntelligentTiering,
		"ONEZONE_IA":          ObjectStorageClassOnezoneIa,
		"GLACIER":             ObjectStorageClassGlacier,
		"DEEP_ARCHIVE":        ObjectStorageClassDeepArchive,
		"RRS":                 ObjectStorageClassReducedRedundancy,
	}
)

func (s StorageClass) ToString() string {
	return StorageClassIndexMap[s]
}

func MatchStorageClassIndex(storageClass string) (StorageClass, error) {
	if index, ok := StorageClassStringMap[storageClass]; ok {
		return index, nil
	} else {

	return 0, ErrInvalidStorageClass

	}
}
