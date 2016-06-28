/* GPLv3 */
/* vim: set ts=4 shiftwidth=4 smarttab noet : */

package main

import (
	"git.letv.cn/zhangdongmao/radoshttpd/rados"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
	"github.com/kataras/iris"
	"github.com/kataras/iris/graceful"
	"github.com/kataras/iris/middleware/recovery"
)

// TODO config file
var (
	LOGPATH                                = "/var/log/yig/yig.log"
	PANIC_LOG_PATH			       = "/var/log/yig/panic.log"
	PIDFILE                                = "/var/run/yig/yig.pid"
	MONTIMEOUT                             = "10"
	OSDTIMEOUT                             = "10"
	BUFFERSIZE                             = 4 << 20 /* 4M */
	AIOCONCURRENT                          = 4
	MAX_CHUNK_SIZE                         = BUFFERSIZE * 2
	STRIPE_UNIT                            = uint(512 << 10) /* 512K */
	OBJECT_SIZE                            = uint(4 << 20)   /* 4M */
	STRIPE_COUNT                           = uint(4)
	CONCURRENT_REQUEST_LIMIT               = 100
	BIND_ADDRESS			       = "0.0.0.0:3000"
	HOST_URL			       = "127.0.0.1" /* should be something like
								s3.lecloud.com
								for production servers
							     */

	/* global variables */
	logger                    *log.Logger
	concurrentRequestNumber LimitedNumber
	Rados			*rados.Conn
)

type LimitedNumber struct {
	size  int
	limit int
	lock  sync.Mutex
}

func (r *LimitedNumber) Init(limit int) {
	r.limit = limit
}

func (r *LimitedNumber) inc() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.size >= r.limit {
		return error("Number limit exceeded.")
	}
	r.size += 1
	return nil
}

func (r *LimitedNumber) dec() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.size -= 1
}

func (r *LimitedNumber) size() int {
	return r.size
}

func LimitRequest(c *iris.Context) {
	if err := concurrentRequestNumber.inc(); err != nil {
		logger.Println("URL:", c.URI(), ", too many concurrent requests")
		c.XML(http.StatusServiceUnavailable, ErrorResponse{
			StatusCode: http.StatusServiceUnavailable,
			Code:"ServiceUnavailable",
			Message:"Too many concurrent requests for this server",
		})
		return
	}
	defer concurrentRequestNumber.dec()
	c.Next()
}

func LogRequest(c *iris.Context)  {
	start := time.Now()
	addr := c.RemoteAddr()
	c.Next()
	logger.Printf("COMPLETE %s %s %s %v %d in %s\n",
		addr, c.Method(), c.Path(), c.Response.StatusCode(),
		c.GetInt("bytesSent"), time.Since(start))
}

func set_stripe_layout(p *rados.StriperPool) int {
	var ret int = 0
	if ret = p.SetLayoutStripeUnit(STRIPE_UNIT); ret < 0 {
		return ret
	}
	if ret = p.SetLayoutObjectSize(OBJECT_SIZE); ret < 0 {
		return ret
	}
	if ret = p.SetLayoutStripeCount(STRIPE_COUNT); ret < 0 {
		return ret
	}
	return ret
}

type RequestContext struct {
	byteSend int64
	requestBody *[]byte
}

func signalHandler()  {
	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGQUIT)

	for {
		signal := <-sigChan
		logger.Printf("Got signal: %v\n", signal)
		switch signal {
		case syscall.SIGQUIT:
			buf := make([]byte, 1<<20)
			runtime.Stack(buf, true)
			logger.Println("=== received SIGQUIT ===")
			logger.Println("*** goroutine dump...")
			logger.Printf("%s\n", buf)
			logger.Println("*** end")
		case syscall.SIGHUP:
			logger.Println("Reloading config file.")
		// TODO: reload config
		}
	}
}

func main() {
	f, err := os.OpenFile(LOGPATH, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file " + LOGPATH)
	}
	defer f.Close()

	logger = log.New(f, "[yig]", log.LstdFlags)

	panicFile, err := os.OpenFile(PANIC_LOG_PATH, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open panic file " + PANIC_LOG_PATH)
	}
	defer panicFile.Close()

	// you must have admin keyring
	Rados, err = rados.NewConn("admin")
	if err != nil {
		panic("failed to open keyring")
	}

	Rados.SetConfigOption("rados_mon_op_timeout", MONTIMEOUT)
	Rados.SetConfigOption("rados_osd_op_timeout", OSDTIMEOUT)

	err = Rados.ReadConfigFile("./conf/ceph.conf")
	if err != nil {
		panic("failed to open ceph.conf")
	}

	err = Rados.Connect()
	if err != nil {
		panic("failed to connect to remote cluster")
	}
	defer Rados.Shutdown()

	go signalHandler()

	concurrentRequestNumber.Init(CONCURRENT_REQUEST_LIMIT)

	apiServer := iris.New()
	apiServer.Use(recovery.New(panicFile))
	apiServer.UseFunc(LogRequest)
	apiServer.UseFunc(LimitRequest)
	apiServer.UseFunc(awsAuth)
	setupHandlers(apiServer)
	logger.Printf("Serving HTTP on %d\n", BIND_ADDRESS)
	graceful.Run(BIND_ADDRESS, 0, apiServer)
}

