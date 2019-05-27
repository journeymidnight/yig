package helper

type Serializable interface {
	Serialize() (map[string]interface{}, error)
	Deserialize(map[string]string) (interface{}, error)
}
