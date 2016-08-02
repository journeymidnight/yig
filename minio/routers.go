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

package minio

import (
	"net/http"

	"git.letv.cn/yig/yig/signature"
	router "github.com/gorilla/mux"
)

// configureServer handler returns final handler for the http server.
func configureServerHandler(c *ServerConfig) http.Handler {
	// Initialize API.
	apiHandlers := objectAPIHandlers{
		ObjectAPI: c.ObjectLayer,
	}

	// Initialize router.
	mux := router.NewRouter()

	// Register all routers.
	registerAPIRouter(mux, apiHandlers)
	// Add new routers here.

	// List of some generic handlers which are applied for all
	// incoming requests.
	var handlerFns = []HandlerFunc{
		// Limits the number of concurrent http requests.
		setRateLimitHandler,
		// TODO: Adds cache control for all browser requests.
		setBrowserCacheControlHandler,
		// CORS setting for all browser API requests.
		setCorsHandler,
		// Validates all incoming URL resources, for invalid/unsupported
		// resources client receives a HTTP error.
		setIgnoreResourcesHandler,
		// Auth handler verifies incoming authorization headers and
		// routes them accordingly. Client receives a HTTP error for
		// invalid/unsupported signatures.
		signature.SetAuthHandler,
		// Add new handlers here.

		setLogHandler,
		// TODO request logger
		// func LogRequest(c *iris.Context)  {
		// start := time.Now()
		// addr := c.RemoteAddr()
		// c.Next()
		// logger.Printf("COMPLETE %s %s %s %v %d in %s\n",
		// addr, c.Method(), c.Path(), c.Response.StatusCode(),
		// c.GetInt("bytesSent"), time.Since(start))
		// }

	}

	// Register rest of the handlers.
	return registerHandlers(mux, handlerFns...)
}
