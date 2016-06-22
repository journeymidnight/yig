/* GPLv3 */
/* vim: set ts=4 shiftwidth=4 smarttab noet : */

package main

import (
	"fmt"
	"git.letv.cn/yig/yig/nettimeout"
	"git.letv.cn/yig/yig/rados"
	"github.com/codegangsta/martini"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// TODO config file
var (
	LOGPATH                                = "/var/log/yig/yig.log"
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
	PORT				       = 3000
	TCP_TIMEOUT			       = 10 /* Seconds */
	HOST_URL			       = "127.0.0.1" /* should be something like
								s3.lecloud.com
								for production servers
							     */

	/* global variables */
	logger                    *log.Logger
	concurrentRequestNumber LimitedNumber
	wg                      sync.WaitGroup
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

func WrapContext() martini.Handler {
	return func(res http.ResponseWriter, req *http.Request, c martini.Context) {
		r := &RequestContext{}
		c.Map(r)
	}
}

func RequestLimit() martini.Handler {
	return func(w http.ResponseWriter, r *http.Request, c martini.Context) {
		/* used for graceful stop */
		wg.Add(1)
		defer wg.Done()

		/* limit concurrent requests */
		if err := concurrentRequestNumber.inc(); err != nil {
			logger.Println("URL:", r.URL, ", too many concurrent requests")
			responseWithError(w, &ErrorResponse{
					StatusCode: http.StatusServiceUnavailable,
					Code:"ServiceUnavailable",
					Message:"Too many concurrent requests for this server",
				})
			return
		}
		defer concurrentRequestNumber.dec()
		c.Next()
	}
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

func createMartini() *martini.ClassicMartini {
	r := martini.NewRouter()
	m := martini.New()
	m.Use(martini.Recovery())
	m.MapTo(r, (*martini.Routes)(nil))
	m.Action(r.Handle)
	return &martini.ClassicMartini{m, r}
}

func main() {
	//log
	f, err := os.OpenFile(LOGPATH, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("failed to open log\n")
	}
	defer f.Close()

	logger = log.New(f, "[yig]", log.LstdFlags)

	m := createMartini()
	m.Use(awsAuth)
	m.Use(WrapContext())
	m.Use(func(w http.ResponseWriter, r *http.Request, context *RequestContext, c martini.Context) {
		start := time.Now()
		addr := r.Header.Get("X-Real-IP")
		if addr == "" {
			addr = r.Header.Get("X-Forwarded-For")
			if addr == "" {
				addr = r.RemoteAddr
			}
			c.Next()
			rw := w.(martini.ResponseWriter)
			logger.Printf("COMPLETE %s %s %s %v %d in %s\n",
				addr, r.Method, r.URL.Path, rw.Status(), context.byteSend, time.Since(start))
		}
	})

	var conn *rados.Conn
	//you must have admin keyring
	conn, err = rados.NewConn("admin")
	if err != nil {
		logger.Println("failed to open keyring")
		return
	}

	conn.SetConfigOption("rados_mon_op_timeout", MONTIMEOUT)
	conn.SetConfigOption("rados_osd_op_timeout", OSDTIMEOUT)

	err = conn.ReadConfigFile("./conf/ceph.conf")
	if err != nil {
		logger.Println("failed to open ceph.conf")
		return
	}

	err = conn.Connect()
	if err != nil {
		logger.Println("failed to connect to remote cluster")
		return
	}
	defer conn.Shutdown()

	m.Map(conn)

	concurrentRequestNumber.Init(CONCURRENT_REQUEST_LIMIT)

	setupHandlers(m)

	// port, read/write timeout
	stoppableListener, err := nettimeout.NewListener(PORT, TCP_TIMEOUT*time.Second, TCP_TIMEOUT*time.Second)
	if err != nil {
		panic(fmt.Sprintf("Failed to listen to %d, quiting\n", PORT))
	}

	server := http.Server{}

	http.HandleFunc("/", m.ServeHTTP)

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT,
		syscall.SIGHUP,
		syscall.SIGQUIT,
		syscall.SIGTERM)

	go server.Serve(stoppableListener)
	logger.Printf("Serving HTTP on port %d\n", PORT)

	for {
		select {
		case signal := <-sigChan:
			logger.Printf("Got signal: %v\n", signal)
			switch signal {
			case syscall.SIGQUIT:
				buf := make([]byte, 1<<20)
				runtime.Stack(buf, true)
				logger.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf)
			case syscall.SIGHUP:
				logger.Println("Reloading config file.")
				// TODO: reload config
			default:
				stoppableListener.Stop()
				logger.Printf("Waiting on server to stop...\n")
				wg.Wait()
				logger.Printf("Server shutdown\n")

				// NOTE(wenjianhn): deferred functions will not run if using os.Exit(0).
				return
			}
		}
	}
}

