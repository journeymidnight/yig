/* GPLv3 */
/* vim: set ts=4 shiftwidth=4 smarttab noet : */

package main

import (
	"github.com/codegangsta/martini"
	"git.letv.cn/yig/yig/nettimeout"
	"git.letv.cn/yig/yig/rados"
	"time"
	"errors"
	"net/http"
    "log"
    "os"
    "os/signal"
    "sync"
    "syscall"
    "runtime"
	"fmt"

)

var (
	LOGPATH                    = "/var/log/yig/yig.log"
    PIDFILE                    = "/var/run/yig/yig.pid"
    QUEUETIMEOUT time.Duration = 5 /* seconds */
    MONTIMEOUT                 = "10"
    OSDTIMEOUT                 = "10"
    BUFFERSIZE                 = 4 << 20 /* 4M */
    AIOCONCURRENT              = 4
    MAX_CHUNK_SIZE             = BUFFERSIZE * 2
    STRIPE_UNIT                = uint(512 << 10) /* 512K */
    OBJECT_SIZE                = uint(4 << 20) /* 4M */
    STRIPE_COUNT               = uint(4)

    /* global variables */
    slog  *log.Logger
    ReqQueue RequestQueue
    wg       sync.WaitGroup
)

/* RequestQueue has two functions */
/* 2. slot is used to queue write/read request */
type RequestQueue struct {
	slots chan bool
}

func (r *RequestQueue) Init(queueLength int) {
	r.slots = make(chan bool, queueLength)
}

func (r *RequestQueue) inc() error {
	select {
	case <-time.After(QUEUETIMEOUT * time.Second):
		return errors.New("Queue is too long, timeout")
	case r.slots <- true:
		/* write to channel to get a slot for writing/reading rados object */
	}
	return nil
}

func (r *RequestQueue) dec() {
	<-r.slots
}

func (r *RequestQueue) size() int {
	return len(r.slots)
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
        slog.Println("URL:", r.URL, "open pool failed")
        ErrorHandler(w, r, http.StatusNotFound)
        return
    }
    defer pool.Destroy()

    striper, err := pool.CreateStriper()
    if err != nil {
        slog.Println("URL:", r.URL, "Create Striper failed")
        ErrorHandler(w, r, http.StatusNotFound)
        return
    }
    defer striper.Destroy()

    size, _, err := striper.State(soid)
    if err != nil {
        slog.Println("URL:%s, failed to get object " + soid, r.URL)
        ErrorHandler(w, r, http.StatusNotFound)
        return
    }
    /* use json format */
    w.Header().Set("Content-Type", "application/json")
    w.Write([]byte(fmt.Sprintf("{\"size\":%d}", size)))
    return
}

func RequestLimit() martini.Handler {
	return func(w http.ResponseWriter, r *http.Request, c martini.Context){
        /* used for graceful stop */
		wg.Add(1)
		defer wg.Done()

        /* limit the max request */
		if err := ReqQueue.inc(); err != nil {
			slog.Println("URL:", r.URL, ", request timeout")
			ErrorHandler(w, r, http.StatusRequestTimeout)
			return
		}
		defer ReqQueue.dec()
		c.Next()
	}
}

func CephStatusHandler(params martini.Params, w http.ResponseWriter, r *http.Request, conn *rados.Conn) {
    c, err := conn.Status()
    if err != nil{
		    ErrorHandler(w, r, 504)
            return
    }
	w.Write([]byte(c))
}


func set_stripe_layout(p * rados.StriperPool) int{
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


func createMartini() *martini.ClassicMartini{
	r := martini.NewRouter()
	m := martini.New()
	m.Use(martini.Recovery())
	m.MapTo(r, (*martini.Routes)(nil))
	m.Action(r.Handle)
	return &martini.ClassicMartini{m, r}
}

func main() {

	var conn  *rados.Conn

	//pid
	/*
	if err := CreatePidfile(PIDFILE); err != nil {
		fmt.Printf("can not create pid file %s\n", PIDFILE) 
		return
	}
	defer RemovePidfile(PIDFILE)
	*/

	//log
	f, err := os.OpenFile(LOGPATH, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("failed to open log\n")
		return
	}
	defer f.Close()


	m := createMartini()
	slog = log.New(f, "[yig]", log.LstdFlags)


    //Redirect stdout and stderr to the log
    syscall.Dup2(int(f.Fd()), 2)
    syscall.Dup2(int(f.Fd()), 1)


	m.Use(WrapContext())
	m.Use(func(w http.ResponseWriter, r *http.Request, conn *rados.Conn, context *ConnnectContext, c martini.Context){
		start := time.Now()
		addr := r.Header.Get("X-Real-IP")
		if addr == "" {
			addr = r.Header.Get("X-Forwarded-For")
			if addr == "" {
				addr = r.RemoteAddr
			}
			c.Next()
			rw := w.(martini.ResponseWriter)
			slog.Printf("COMPLETE %s %s %s %v %d in %s\n", addr, r.Method, r.URL.Path, rw.Status(), context.byteSend, time.Since(start))
	}})

	//you must have admin keyring
	conn, err = rados.NewConn("admin")
	if err != nil {
		slog.Println("failed to open keyring")
		return
	}

	conn.SetConfigOption("rados_mon_op_timeout", MONTIMEOUT)
	conn.SetConfigOption("rados_osd_op_timeout", OSDTIMEOUT)

	err = conn.ReadConfigFile("./conf/ceph.conf")
	if err != nil {
		slog.Println("failed to open ceph.conf")
		return
	}

	err = conn.Connect()
	if err != nil {
		slog.Println("failed to connect to remote cluster")
		return
	}
	defer conn.Shutdown()

    m.Map(conn)

	ReqQueue.Init(100)

	m.Get("/threads", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, fmt.Sprintf("%d\n", ReqQueue.size()))
	})
	m.Get("/cephstatus", RequestLimit, CephStatusHandler)

    m.Get("/info/(?P<pool>[A-Za-z0-9]+)/(?P<soid>[^/]+)", RequestLimit(), InfoHandler)


	//port 3000, read/write timeout 10 seconds
	sl, err := nettimeout.NewListener(3000, time.Duration(10) * time.Second,
						time.Duration(10) * time.Second);
	if err != nil {
		fmt.Printf("Failed to listen to %d, quiting\n", 3000)
		os.Stdout.Sync()
		slog.Printf("Failed to listen to %d, quiting", 3000)
		return
	}

	server := http.Server{}

	http.HandleFunc("/", m.ServeHTTP)

	sigChan := make(chan os.Signal)
	signal.Notify(sigChan, syscall.SIGINT,
		syscall.SIGHUP,
		syscall.SIGQUIT,
		syscall.SIGTERM)

	go func() {
		server.Serve(sl)
	}()

	fmt.Println("fuck")
	slog.Printf("Serving HTTP\n")

	for {
		select {
		case signal := <-sigChan:
			slog.Printf("Got signal:%v\n", signal)
			switch signal {
			case syscall.SIGQUIT:
				buf := make([]byte, 1<<20)
				runtime.Stack(buf, true)
				slog.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf)
			case syscall.SIGHUP:
				slog.Println("Reloading config file.")
				/*do some thing*/
			default:
				sl.Stop()
				slog.Printf("Waiting on server\n")
				wg.Wait()
				slog.Printf("Server shutdown\n")

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

