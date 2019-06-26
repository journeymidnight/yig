/*
 * Minio Cloud Storage, (C) 2015, 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"errors"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	router "github.com/gorilla/mux"
	"github.com/journeymidnight/yig/api"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/storage"
)

type ServerConfig struct {
	Address      string
	KeyFilePath  string      // path for SSL key file
	CertFilePath string      // path for SSL certificate file
	Logger       *log.Logger // global logger
	ObjectLayer  *storage.YigStorage
}

// configureServer handler returns final handler for the http server.
func configureServerHandler(c *ServerConfig) http.Handler {
	// Initialize API.
	apiHandlers := api.ObjectAPIHandlers{
		ObjectAPI: c.ObjectLayer,
	}

	// Initialize router.
	mux := router.NewRouter()

	// Register all routers.
	api.RegisterAPIRouter(mux, apiHandlers)
	// Add new routers here.

	// List of some generic handlers which are applied for all
	// incoming requests.
	var handlerFns = []api.HandlerFunc{
		// Limits the number of concurrent http requests.
		api.SetCommonHeaderHandler,
		// CORS setting for all browser API requests.
		api.SetCorsHandler,
		// Validates all incoming URL resources, for invalid/unsupported
		// resources client receives a HTTP error.
		api.SetIgnoreResourcesHandler,
		// Auth handler verifies incoming authorization headers and
		// routes them accordingly. Client receives a HTTP error for
		// invalid/unsupported signatures.
		api.SetAuthHandler,
		// Add new handlers here.

		api.SetLogHandler,

		api.NewAccessLogHandler,

		// This handler must be last one.
		api.SetGenerateContextHandler,
	}

	// Register rest of the handlers.
	return api.RegisterHandlers(mux, c.ObjectLayer.MetaStorage, handlerFns...)
}

// configureServer configure a new server instance
func configureServer(c *ServerConfig) *api.Server {
	apiServer := &api.Server{
		Server: &http.Server{
			Addr: c.Address,
			// Adding timeout of 10 minutes for unresponsive client connections.
			ReadTimeout:    10 * time.Minute,
			WriteTimeout:   10 * time.Minute,
			Handler:        configureServerHandler(c),
			MaxHeaderBytes: 1 << 20,
		},
	}
	apiServer.Server.SetKeepAlivesEnabled(helper.CONFIG.KeepAlive)

	// Returns configured HTTP server.
	return apiServer
}

// getListenIPs - gets all the ips to listen on.
func getListenIPs(httpServerConf *http.Server) (hosts []string, port string) {
	host, port, err := net.SplitHostPort(httpServerConf.Addr)
	helper.FatalIf(err, "Unable to parse host port.")

	switch {
	case host != "":
		hosts = append(hosts, host)
	default:
		addrs, err := net.InterfaceAddrs()
		helper.FatalIf(err, "Unable to determine network interface address.")
		for _, addr := range addrs {
			if addr.Network() == "ip+net" {
				host := strings.Split(addr.String(), "/")[0]
				if ip := net.ParseIP(host); ip.To4() != nil {
					hosts = append(hosts, host)
				}
			}
		}
	}
	return hosts, port
}

// Print listen ips.
func printListenIPs(tls bool, hosts []string, port string) {
	for _, host := range hosts {
		if tls {
			logger.Printf(5, "    https://%s:%s\n", host, port)
		} else {
			logger.Printf(5, "    http://%s:%s\n", host, port)
		}
	}
}

// Extract port number from address address should be of the form host:port.
func getPort(address string) int {
	_, portStr, err := net.SplitHostPort(address)
	helper.FatalIf(err, "Unable to parse host port.")
	portInt, err := strconv.Atoi(portStr)
	helper.FatalIf(err, "Invalid port number.")
	return portInt
}

// Make sure that none of the other processes are listening on the
// specified port on any of the interfaces.
//
// On linux if a process is listening on 127.0.0.1:9000 then Listen()
// on ":9000" fails with the error "port already in use".
// However on macOS Listen() on ":9000" falls back to the IPv6 address.
// This causes confusion on macOS that minio server is not reachable
// on 127.0.0.1 even though minio server is running. So before we start
// the minio server we make sure that the port is free on all the IPs.
func checkPortAvailability(port int) {
	isAddrInUse := func(err error) bool {
		// Check if the syscall error is EADDRINUSE.
		// EADDRINUSE is the system call error if another process is
		// already listening at the specified port.
		neterr, ok := err.(*net.OpError)
		if !ok {
			return false
		}
		osErr, ok := neterr.Err.(*os.SyscallError)
		if !ok {
			return false
		}
		sysErr, ok := osErr.Err.(syscall.Errno)
		if !ok {
			return false
		}
		if sysErr != syscall.EADDRINUSE {
			return false
		}
		return true
	}
	ifcs, err := net.Interfaces()
	if err != nil {
		helper.FatalIf(err, "Unable to list interfaces.")
	}
	for _, ifc := range ifcs {
		addrs, err := ifc.Addrs()
		if err != nil {
			helper.FatalIf(err, "Unable to list addresses on interface %s.", ifc.Name)
		}
		for _, addr := range addrs {
			ipnet, ok := addr.(*net.IPNet)
			if !ok {
				helper.ErrorIf(errors.New(""), "Failed to assert type on (*net.IPNet) interface.")
				continue
			}
			ip := ipnet.IP
			network := "tcp4"
			if ip.To4() == nil {
				network = "tcp6"
			}
			tcpAddr := net.TCPAddr{IP: ip, Port: port, Zone: ifc.Name}
			l, err := net.ListenTCP(network, &tcpAddr)
			if err != nil {
				if isAddrInUse(err) {
					// Fail if port is already in use.
					helper.FatalIf(err, "Unable to listen on %s:%.d.", tcpAddr.IP, tcpAddr.Port)
				} else {
					// Ignore other errors.
					continue
				}
			}
			if err = l.Close(); err != nil {
				helper.FatalIf(err, "Unable to close listener on %s:%.d.", tcpAddr.IP, tcpAddr.Port)
			}
		}
	}
}

func isSSL(c *ServerConfig) bool {
	if helper.FileExists(c.KeyFilePath) && helper.FileExists(c.CertFilePath) {
		return true
	}
	return false
}

var ApiServer *api.Server

// blocks after server started
func startApiServer(c *ServerConfig) {
	serverAddress := c.Address

	host, port, _ := net.SplitHostPort(serverAddress)
	// If port empty, default to port '80'
	if port == "" {
		port = "80"
		// if SSL is enabled, choose port as "443" instead.
		if isSSL(c) {
			port = "443"
		}
	}

	// Check if requested port is available.
	checkPortAvailability(getPort(net.JoinHostPort(host, port)))

	// Configure server.
	apiServer := configureServer(c)

	hosts, port := getListenIPs(apiServer.Server) // get listen ips and port.
	tls := apiServer.Server.TLSConfig != nil      // 'true' if TLS is enabled.

	logger.Println(5, "\nS3 Object Storage:")
	// Print api listen ips.
	printListenIPs(tls, hosts, port)

	go func() {
		var err error
		// Configure TLS if certs are available.
		if isSSL(c) {
			err = apiServer.Server.ListenAndServeTLS(c.CertFilePath, c.KeyFilePath)
		} else {
			// Fallback to http.
			err = apiServer.Server.ListenAndServe()
		}
		helper.FatalIf(err, "API server error.")
	}()
}

func stopApiServer() {
	ApiServer.Stop()
}
