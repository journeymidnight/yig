/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package api

import (
	"git.letv.cn/yig/yig/helper"
	"net/http"
	"sync"
	"time"
)

var rateLimiter *rateLimit

// rateLimit performs both concurrent request limit and graceful shutdown
type rateLimit struct {
	handler         http.Handler
	currentRequests int
	requestLimit    int
	lock            *sync.Mutex
}

// ServeHTTP is an http.Handler ServeHTTP method, implemented to rate
// limit incoming HTTP requests.
func (l *rateLimit) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	l.lock.Lock()
	if l.currentRequests+1 > l.requestLimit {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("Server too busy"))
		l.lock.Unlock()
		return
	}
	l.currentRequests += 1
	l.lock.Unlock()

	l.handler.ServeHTTP(w, r)

	l.lock.Lock()
	l.currentRequests -= 1
	l.lock.Unlock()
}

func (l *rateLimit) ShutdownServer() {
	l.lock.Lock()
	l.requestLimit = 0
	l.lock.Unlock()

	for {
		time.Sleep(1 * time.Second)
		l.lock.Lock()
		helper.Logger.Print("Remaining requests:", l.currentRequests)
		if l.currentRequests == 0 {
			// deliberately leave the lock locked
			return
		}
		l.lock.Unlock()
	}
}

// setRateLimitHandler limits the number of concurrent http requests based on
// CONFIG.ConcurrentRequestLimit
func SetRateLimitHandler(handler http.Handler, _ ObjectLayer) http.Handler {
	rateLimiter = &rateLimit{
		handler:         handler,
		currentRequests: 0,
		requestLimit:    helper.CONFIG.ConcurrentRequestLimit,
		lock:            new(sync.Mutex),
	}
	return rateLimiter
}
