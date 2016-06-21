/* GPLv3 */
/* vim: set ts=4 shiftwidth=4 smarttab noet : */

package main

import (
	"errors"
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
		r := &ConnnectContext{0}
		c.Map(r)
	}
}

func InfoHandler(params martini.Params, w http.ResponseWriter, r *http.Request, conn *rados.Conn) {
	poolname := params["pool"]
	soid := params["soid"]
	pool, err := conn.OpenPool(poolname)
	if err != nil {
		logger.Println("URL:", r.URL, "open pool failed")
		ErrorHandler(w, r, http.StatusNotFound)
		return
	}
	defer pool.Destroy()

	striper, err := pool.CreateStriper()
	if err != nil {
		logger.Println("URL:", r.URL, "Create Striper failed")
		ErrorHandler(w, r, http.StatusNotFound)
		return
	}
	defer striper.Destroy()

	size, _, err := striper.State(soid)
	if err != nil {
		logger.Println("URL:%s, failed to get object "+soid, r.URL)
		ErrorHandler(w, r, http.StatusNotFound)
		return
	}
	/* use json format */
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(fmt.Sprintf("{\"size\":%d}", size)))
	return
}

func RequestLimit() martini.Handler {
	return func(w http.ResponseWriter, r *http.Request, c martini.Context) {
		/* used for graceful stop */
		wg.Add(1)
		defer wg.Done()

		/* limit concurrent requests */
		if err := concurrentRequestNumber.inc(); err != nil {
			logger.Println("URL:", r.URL, ", too many concurrent requests")
			ErrorHandler(w, r, http.StatusServiceUnavailable)
			return
		}
		defer concurrentRequestNumber.dec()
		c.Next()
	}
}

func CephStatusHandler(params martini.Params, w http.ResponseWriter, r *http.Request, conn *rados.Conn) {
	c, err := conn.Status()
	if err != nil {
		ErrorHandler(w, r, 504)
		return
	}
	w.Write([]byte(c))
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

/* this could be a context */
type ConnnectContext struct {
	byteSend int64
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
	m.Use(WrapContext())
	m.Use(func(w http.ResponseWriter, r *http.Request, conn *rados.Conn, context *ConnnectContext, c martini.Context) {
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

	m.Get("/threads", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, fmt.Sprintf("%d\n", concurrentRequestNumber.size()))
	})
	m.Get("/cephstatus", RequestLimit(), CephStatusHandler)

	m.Get("/info/(?P<pool>[A-Za-z0-9]+)/(?P<soid>[^/]+)", RequestLimit(), InfoHandler)

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

func ErrorHandler(w http.ResponseWriter, r *http.Request, status int) {
	switch status {
	case http.StatusForbidden:
		w.WriteHeader(status)
		w.Write([]byte("Forbidden"))
	case http.StatusNotFound:
		w.WriteHeader(status)
		w.Write([]byte("object not found"))
	case http.StatusRequestTimeout:
		w.WriteHeader(status)
		w.Write([]byte("server is too busy,timeout"))
	case http.StatusUnauthorized:
		w.WriteHeader(status)
		w.Write([]byte("UnAuthorized"))
	case http.StatusInternalServerError:
		w.WriteHeader(status)
		w.Write([]byte("Internal Server Error"))
	}
}
