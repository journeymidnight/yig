package datatype

import (
	"encoding/xml"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	"io"
	"io/ioutil"
)

type Tagging struct {
	XMLName xml.Name `xml:"Tagging"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	TagSet  TagSet   `xml:"TagSet"`
}

type TagSet struct {
	XMLName xml.Name `xml:"TagSet"`
	Tag     []Tag    `xml:"Tag"`
}

type Tag struct {
	XMLName xml.Name `xml:"Tag"`
	Key     string   `xml:"Key"`
	Value   string   `xml:"Value"`
}

type TaggingData struct {
	Tagging map[string]string
}

func (w *Tagging) parseObject() (map[string]string, error) {
	taggingData := make(map[string]string)
	if len(w.TagSet.Tag) > 10 {
		return taggingData, ErrMalformedTaggingConfiguration
	}
	for _, tag := range w.TagSet.Tag {
		if len(tag.Key) > 128 {
			return taggingData, ErrInvalidTag
		}
		if len(tag.Value) > 256 {
			return taggingData, ErrInvalidTag
		}
		taggingData[tag.Key] = tag.Value
	}
	return taggingData, nil
}

func ParseObjectTaggingConfig(reader io.Reader) (taggingDataReq *TaggingData, err error) {
	taggingConfig := new(Tagging)
	taggingDataReq = new(TaggingData)
	taggingBuffer, err := ioutil.ReadAll(reader)
	if err != nil {
		helper.Logger.Error("Unable to read Tagging setting body:", err)
		return taggingDataReq, err
	}
	err = xml.Unmarshal(taggingBuffer, taggingConfig)
	if err != nil {
		helper.Logger.Error("Unable to parse metadata XML body:", err)
		return taggingDataReq, ErrMalformedTaggingConfiguration
	}
	taggingDataReq.Tagging, err = taggingConfig.parseObject()
	if err != nil {
		return taggingDataReq, err
	}
	return taggingDataReq, nil
}

func MarShalObjectTagging(data TaggingData) ([]byte, error) {
	result := new(Tagging)
	result.TagSet = TagSet{}
	result.TagSet.Tag = []Tag{}
	for key, value := range data.Tagging {
		tag := new(Tag)
		tag.Key = key
		tag.Value = value
		result.TagSet.Tag = append(result.TagSet.Tag, *tag)
	}
	resultBytes, err := xml.Marshal(result)
	if err != nil {
		return nil, ErrMalformedTaggingConfiguration
	}
	return resultBytes, nil
}
