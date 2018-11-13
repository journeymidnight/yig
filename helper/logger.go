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

package helper

import (
	"bufio"
	"bytes"
	"github.com/dustin/go-humanize"
	"github.com/journeymidnight/yig/log"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
)

var Logger *log.Logger

// sysInfo returns useful system statistics.
func sysInfo() map[string]string {
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}
	memstats := &runtime.MemStats{}
	runtime.ReadMemStats(memstats)
	return map[string]string{
		"host.name":      host,
		"host.os":        runtime.GOOS,
		"host.arch":      runtime.GOARCH,
		"host.lang":      runtime.Version(),
		"host.cpus":      strconv.Itoa(runtime.NumCPU()),
		"mem.used":       humanize.Bytes(memstats.Alloc),
		"mem.total":      humanize.Bytes(memstats.Sys),
		"mem.heap.used":  humanize.Bytes(memstats.HeapAlloc),
		"mem.heap.total": humanize.Bytes(memstats.HeapSys),
	}
}

// stackInfo returns printable stack trace.
func stackInfo() string {
	// Convert stack-trace bytes to io.Reader.
	rawStack := bufio.NewReader(bytes.NewBuffer(debug.Stack()))
	// Skip stack trace lines until our real caller.
	for i := 0; i <= 4; i++ {
		rawStack.ReadLine()
	}

	// Read the rest of useful stack trace.
	stackBuf := new(bytes.Buffer)
	stackBuf.ReadFrom(rawStack)

	// Strip GOPATH of the build system and return.
	return strings.Replace(stackBuf.String(), "src/", "", -1)
}

// errorIf synonymous with fatalIf but doesn't exit on error != nil
func ErrorIf(err error, msg string, data ...interface{}) {
	if err == nil {
		return
	}
	Logger.Printf(5, msg, data...)
	Logger.Println(5, "With error: ", err.Error())
	Logger.Println(5, "System Info: ", sysInfo())
}

// fatalIf wrapper function which takes error and prints error messages.
func FatalIf(err error, msg string, data ...interface{}) {
	if err == nil {
		return
	}
	Logger.Printf(5, msg, data...)
	Logger.Println(5, "With error: ", err.Error())
	Logger.Println(5, "System Info: ", sysInfo())
	Logger.Println(5, "Stack trace: ", stackInfo())
	os.Exit(1)
}
