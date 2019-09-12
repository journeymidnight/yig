package seaweed

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/journeymidnight/seaweedfs/weed/operation"
	"github.com/journeymidnight/seaweedfs/weed/wdclient"
	"github.com/journeymidnight/yig/backend"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta/types"
	"google.golang.org/grpc"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

// 30M, more precisely (32M - 2B), limit introduced by cannyls
const ObjectSizeLimit = 30 << 20

type uploadResult struct {
	Name  string `json:"name,omitempty"`
	Size  uint32 `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
	ETag  string `json:"eTag,omitempty"`
}

// SeaweedfsCluster implements yig.backend.backend
type SeaweedfsCluster struct {
	logger        *log.Logger
	masters       []string
	seaweedClient *wdclient.MasterClient
	httpClient    *http.Client
}

func Initialize(logger *log.Logger, config helper.Config) map[string]backend.Cluster {
	clientID := fmt.Sprintf("YIG-%s", config.InstanceId)
	clusters := make(map[string]backend.Cluster)
	for _, masters := range config.SeaweedMasters {
		storage := NewSeaweedStorage(logger, clientID, masters)
		clusters[storage.ID()] = storage
	}
	return clusters
}

func PickCluster(clusters map[string]backend.Cluster, weights map[string]int,
	size uint64, class types.StorageClass,
	objectType types.ObjectType) (cluster backend.Cluster, pool string, err error) {

	// TODO
	for _, cluster := range clusters {
		return cluster, "", nil
	}
	return nil, "", errors.New("no seaweedfs cluster configured")
}

func NewSeaweedStorage(logger *log.Logger, clientID string, masters []string) SeaweedfsCluster {
	logger.Logger.Println("Initializing Seaweedfs client:", clientID,
		"masters:", masters)
	seaweedClient := wdclient.NewMasterClient(context.Background(),
		grpc.WithInsecure(), clientID, masters)
	go seaweedClient.KeepConnectedToMaster()
	seaweedClient.WaitUntilConnected() // FIXME some kind of timeout?
	logger.Logger.Println("Seaweedfs client initialized for", masters)
	return SeaweedfsCluster{
		logger:        logger,
		masters:       masters,
		seaweedClient: seaweedClient,
		httpClient: &http.Client{
			Timeout: 3 * time.Minute,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 8192, // keep long connections with volume server
			},
		},
	}
}

func (s SeaweedfsCluster) ID() string {
	return strings.Join(s.masters, ",")
}

func (s SeaweedfsCluster) GetUsage() (usage backend.Usage, err error) {
	// TODO
	return
}

func (s SeaweedfsCluster) assignObject(poolName string) (result operation.AssignResult, err error) {
	masterAddress := s.seaweedClient.GetMaster()
	assignRequest := &operation.VolumeAssignRequest{
		// TODO read from config
		Count:       1,
		Replication: "002",
		Collection:  poolName,
		Ttl:         "",
		DataCenter:  "",
	}
	assignResult, err := operation.Assign(masterAddress, nil,
		assignRequest)
	if err != nil {
		return operation.AssignResult{}, err
	}
	if assignResult.Error != "" {
		return operation.AssignResult{}, errors.New(assignResult.Error)
	}
	return *assignResult, nil
}

func (s SeaweedfsCluster) formUploadBody(object io.Reader, objectName string) (body *bytes.Buffer,
	contentType string, bytesWritten uint64, err error) {

	// limit object size because of cannyls
	object = io.LimitReader(object, ObjectSizeLimit)
	body = &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", objectName)
	if err != nil {
		s.logger.Logger.Println("CreateFormFile error:", err)
		return nil, "", 0, err
	}
	n, err := io.Copy(part, object)
	if err != nil {
		s.logger.Logger.Println("io.Copy error:", err)
		return nil, "", 0, err
	}
	err = writer.Close()
	if err != nil {
		s.logger.Logger.Println("writer.Close error:", err)
		return nil, "", 0, err
	}
	return body, writer.FormDataContentType(), uint64(n), nil
}

func (s SeaweedfsCluster) Put(poolName string, object io.Reader) (objectName string,
	bytesWritten uint64, err error) {

	assigned, err := s.assignObject(poolName)
	if err != nil {
		s.logger.Logger.Println("assignObject error:", err)
		return "", 0, err
	}
	url := fmt.Sprintf("http://%s/%s", assigned.Url, assigned.Fid)

	body, contentType, bytesWritten, err := s.formUploadBody(object, assigned.Fid)
	if err != nil {
		s.logger.Logger.Println("formUploadBody error:", err)
		return "", 0, err
	}
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		s.logger.Logger.Println("http.NewRequest error:", err)
		return "", 0, err
	}
	req.Header.Set("Content-Type", contentType)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Logger.Println("s.httpClient.Do error:", err)
		return "", 0, err
	}
	var result uploadResult
	err = helper.ReadJsonBody(resp.Body, &result)
	if err != nil {
		s.logger.Logger.Println("ReadJsonBody error:", err)
		return "", 0, err
	}
	if result.Error != "" {
		return "", 0, errors.New(result.Error)
	}
	return assigned.Fid, bytesWritten, nil
}

func (s SeaweedfsCluster) Append(poolName, existName string, objectChunk io.Reader,
	offset int64) (objectName string, bytesWritten uint64, err error) {

	var url string
	if len(existName) == 0 {
		assigned, err := s.assignObject(poolName)
		if err != nil {
			s.logger.Logger.Println("assignObject error:", err)
			return "", 0, err
		}
		url = fmt.Sprintf("http://%s/%s", assigned.Url, assigned.Fid)
		objectName = assigned.Fid
	} else {
		url, err = s.seaweedClient.LookupFileId(existName)
		if err != nil {
			s.logger.Logger.Println("seaweedClient.LookupFileId error:", err)
			return "", 0, err
		}
		objectName = existName
	}

	body, contentType, bytesWritten, err := s.formUploadBody(objectChunk, objectName)
	if err != nil {
		s.logger.Logger.Println("formUploadBody error:", err)
		return "", 0, err
	}
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		s.logger.Logger.Println("http.NewRequest error:", err)
		return "", 0, err
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-Start-Offset", fmt.Sprint(offset))
	req.Header.Set("X-Reservation", fmt.Sprint(ObjectSizeLimit))

	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Logger.Println("s.httpClient.Do error:", err)
		return "", 0, err
	}
	var result uploadResult
	err = helper.ReadJsonBody(resp.Body, &result)
	if err != nil {
		s.logger.Logger.Println("ReadJsonBody error:", err)
		return "", 0, err
	}
	if result.Error != "" {
		return "", 0, errors.New(result.Error)
	}
	return objectName, bytesWritten, nil
}

func (s SeaweedfsCluster) GetReader(poolName, objectName string,
	offset int64, length uint64) (reader io.ReadCloser, err error) {

	url, err := s.seaweedClient.LookupFileId(objectName)
	if err != nil {
		s.logger.Logger.Println("seaweedClient.LookupFileId error:", err)
		return nil, err
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	if length != 0 {
		req.Header.Set("X-Start-Offset", fmt.Sprint(offset))
		req.Header.Set("X-Length-Required", fmt.Sprint(length))
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Logger.Println("httpClient.Get error:", err)
		return nil, err
	}
	return resp.Body, nil
}

// Corresponding to weed/server/volume_server_handlers_write.go,
// function DeleteHandler, writeDeleteResult
func (s SeaweedfsCluster) Remove(poolName, objectName string) (err error) {
	url, err := s.seaweedClient.LookupFileId(objectName)
	if err != nil {
		s.logger.Logger.Println("seaweedClient.LookupFileId error:", err)
		return err
	}
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		s.logger.Logger.Println("http.NewRequest error:", err)
		return err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		s.logger.Logger.Println("httpClient.Get error:", err)
		return err
	}
	var result map[string]interface{}
	err = helper.ReadJsonBody(resp.Body, &result)
	if err != nil {
		s.logger.Logger.Println("ReadJsonBody error:", err)
		return err
	}
	if resp.StatusCode == http.StatusAccepted {
		return nil
	}
	return errors.New(fmt.Sprintln(result["error"]))
}
