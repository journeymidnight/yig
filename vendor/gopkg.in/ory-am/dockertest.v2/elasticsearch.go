package dockertest

import (
	"errors"
	"fmt"
	"log"
	"time"
)

// SetupElasticSearchContainer sets up a real ElasticSearch instance for testing purposes
// using a Docker container. It returns the container ID and its IP address,
// or makes the test fail on error.
func SetupElasticSearchContainer() (c ContainerID, ip string, port int, err error) {
	port = RandomPort()
	forward := fmt.Sprintf("%d:%d", port, 9200)
	if BindDockerToLocalhost != "" {
		forward = "127.0.0.1:" + forward
	}
	c, ip, err = SetupContainer(ElasticSearchImageName, port, 15*time.Second, func() (string, error) {
		return run("--name", GenerateContainerID(), "-d", "-P", "-p", forward, ElasticSearchImageName)
	})
	return
}

// ConnectToElasticSearch starts an ElasticSearch image and passes the database url to the connector callback function.
// The url will match the ip:port pattern (e.g. 123.123.123.123:4241)
func ConnectToElasticSearch(tries int, delay time.Duration, connector func(url string) bool) (c ContainerID, err error) {
	c, ip, port, err := SetupElasticSearchContainer()
	if err != nil {
		return c, fmt.Errorf("Could not set up ElasticSearch container: %v", err)
	}

	for try := 0; try <= tries; try++ {
		time.Sleep(delay)
		url := fmt.Sprintf("%s:%d", ip, port)
		if connector(url) {
			return c, nil
		}
		log.Printf("Try %d failed. Retrying.", try)
	}
	return c, errors.New("Could not set up ElasticSearch container.")
}
