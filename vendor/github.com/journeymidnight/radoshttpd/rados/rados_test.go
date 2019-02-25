package rados

import "testing"
//import "bytes"
import "github.com/stretchr/testify/assert"
import "os"
import "os/exec"
import "io"
import "io/ioutil"
import "time"
import "net"
import "fmt"

func GetUUID() string {
    out, _ := exec.Command("uuidgen").Output()
    return string(out[:36])
}

func TestVersion(t *testing.T) {
    var major, minor, patch = Version()
    assert.False(t, major < 0 || major > 1000, "invalid major")
    assert.False(t, minor < 0 || minor > 1000, "invalid minor")
    assert.False(t, patch < 0 || patch > 1000, "invalid patch")
}

func TestGetSetConfigOption(t *testing.T) {
    conn, _ := NewConn("admin")

    // rejects invalid options
    err := conn.SetConfigOption("wefoijweojfiw", "welfkwjelkfj")
    assert.Error(t, err, "Invalid option")

    // verify SetConfigOption changes a values
    log_file_val, err := conn.GetConfigOption("log_file")
    assert.NotEqual(t, log_file_val, "/dev/null")

    err = conn.SetConfigOption("log_file", "/dev/null")
    assert.NoError(t, err, "Invalid option")

    log_file_val, err = conn.GetConfigOption("log_file")
    assert.Equal(t, log_file_val, "/dev/null")
}

func TestParseDefaultConfigEnv(t *testing.T) {
    conn, _ := NewConn("admin")

    log_file_val, _ := conn.GetConfigOption("log_file")
    assert.NotEqual(t, log_file_val, "/dev/null")

    err := os.Setenv("CEPH_ARGS", "--log-file /dev/null")
    assert.NoError(t, err)

    err = conn.ParseDefaultConfigEnv()
    assert.NoError(t, err)

    log_file_val, _ = conn.GetConfigOption("log_file")
    assert.Equal(t, log_file_val, "/dev/null")
}

func TestParseCmdLineArgs(t *testing.T) {
    conn, _ := NewConn("admin")
    conn.ReadDefaultConfigFile()

    mon_host_val, _ := conn.GetConfigOption("mon_host")
    assert.NotEqual(t, mon_host_val, "1.1.1.1")

    args := []string{ "--mon-host", "1.1.1.1" }
    err := conn.ParseCmdLineArgs(args)
    assert.NoError(t, err)

    mon_host_val, _ = conn.GetConfigOption("mon_host")
    assert.Equal(t, mon_host_val, "1.1.1.1")
}

func TestGetClusterStats(t *testing.T) {
    conn, _ := NewConn("admin")
    conn.ReadDefaultConfigFile()
    conn.Connect()

    poolname := GetUUID()
    err := conn.MakePool(poolname)
    assert.NoError(t, err)

    pool, err := conn.OpenPool(poolname)
    assert.NoError(t, err)

    buf := make([]byte, 1<<22)
    pool.Write("obj", buf, 0)

    for i := 0; i < 30; i++ {
        stat, err := conn.GetClusterStats()
        assert.NoError(t, err)

        // wait a second if stats are zero
        if stat.Kb == 0 || stat.Kb_used == 0 ||
            stat.Kb_avail == 0 || stat.Num_objects == 0 {
            fmt.Println("waiting for cluster stats to refresh")
            time.Sleep(time.Second)
        } else {
            // success
            conn.Shutdown()
            return
        }
    }

    t.Error("Cluster stats are zero")

    conn.Shutdown()
}

func TestGetFSID(t *testing.T) {
    conn, _ := NewConn("admin")
    conn.ReadDefaultConfigFile()
    conn.Connect()

    fsid, err := conn.GetFSID()
    assert.NoError(t, err)
    assert.NotEqual(t, fsid, "")

    conn.Shutdown()
}

func TestGetInstanceID(t *testing.T) {
    conn, _ := NewConn("admin")
    conn.ReadDefaultConfigFile()
    conn.Connect()

    id := conn.GetInstanceID()
    assert.NotEqual(t, id, 0)

    conn.Shutdown()
}

func TestMakeDeletePool(t *testing.T) {
    conn, _ := NewConn("admin")
    conn.ReadDefaultConfigFile()
    conn.Connect()

    // get current list of pool
    pools, err := conn.ListPools()
    assert.NoError(t, err)

    // check that new pool name is unique
    new_name := GetUUID()
    for _, poolname := range pools {
        if new_name == poolname {
            t.Error("Random pool name exists!")
            return
        }
    }

    // create pool
    err = conn.MakePool(new_name)
    assert.NoError(t, err)

    // get updated list of pools
    pools, err = conn.ListPools()
    assert.NoError(t, err)

    // verify that the new pool name exists
    found := false
    for _, poolname := range pools {
        if new_name == poolname {
            found = true
        }
    }

    if !found {
        t.Error("Cannot find newly created pool")
    }

    // delete the pool
    err = conn.DeletePool(new_name)
    assert.NoError(t, err)

    // verify that it is gone

    // get updated list of pools
    pools, err = conn.ListPools()
    assert.NoError(t, err)

    // verify that the new pool name exists
    found = false
    for _, poolname := range pools {
        if new_name == poolname {
            found = true
        }
    }

    if found {
        t.Error("Deleted pool still exists")
    }

    conn.Shutdown()
}

func TestPingMonitor(t *testing.T) {
    conn, _ := NewConn("admin")
    conn.ReadDefaultConfigFile()
    conn.Connect()

    // mon id that should work with vstart.sh
    reply, err := conn.PingMonitor("mon")
    if err == nil {
        assert.NotEqual(t, reply, "")
        return
    }

    // try to use a hostname as the monitor id
    mon_addr, _ := conn.GetConfigOption("mon_host")
    hosts, _ := net.LookupAddr(mon_addr)
    for _, host := range hosts {
        reply, err := conn.PingMonitor(host)
        if err == nil {
            assert.NotEqual(t, reply, "")
            return
        }
    }

    t.Error("Could not find a valid monitor id")

    conn.Shutdown()
}

func TestReadConfigFile(t *testing.T) {
    conn, _ := NewConn("admin")

    // check current log_file value
    log_file_val, err := conn.GetConfigOption("log_file")
    assert.NoError(t, err)
    assert.NotEqual(t, log_file_val, "/dev/null")

    // create a temporary ceph.conf file that changes the log_file conf
    // option.
    file, err := ioutil.TempFile("/tmp", "go-rados")
    assert.NoError(t, err)

    _, err = io.WriteString(file, "[global]\nlog_file = /dev/null\n")
    assert.NoError(t, err)

    // parse the config file
    err = conn.ReadConfigFile(file.Name())
    assert.NoError(t, err)

    // check current log_file value
    log_file_val, err = conn.GetConfigOption("log_file")
    assert.NoError(t, err)
    assert.Equal(t, log_file_val, "/dev/null")

    // cleanup
    file.Close()
    os.Remove(file.Name())
}

func TestWaitForLatestOSDMap(t *testing.T) {
    conn, _ := NewConn("admin")
    conn.ReadDefaultConfigFile()
    conn.Connect()

    err := conn.WaitForLatestOSDMap()
    assert.NoError(t, err)

    conn.Shutdown()
}


type IoCtxWrapper struct {
	oid string
	striper * StriperPool
	offset int
}


func NewIoCtxWrapper(oid string, striper * StriperPool) *IoCtxWrapper{
	return &IoCtxWrapper{oid, striper, 0}
}

func (wrapper *IoCtxWrapper) Write(d []byte) (int, error) {

	n, err := wrapper.striper.Write(wrapper.oid, d, uint64(wrapper.offset))

	if err != nil {
		return n, err
	} else {
		wrapper.offset +=  len(d)
	}
	return len(d), err

}

func TestUploadDownloadCheckFile(t * testing.T) {

    conf_file_path := "/etc/ceph/ceph.conf"
    test_file_path := "./README.md"

    conn, _ := NewConn("admin")
    err := conn.ReadConfigFile(conf_file_path)
    assert.NoError(t, err)

    fmt.Println("connecting")
    err = conn.Connect()
    assert.NoError(t, err)


    poolname := GetUUID()
    err = conn.MakePool(poolname)
    assert.NoError(t, err)


    pool, err := conn.OpenPool(poolname)
    assert.NoError(t, err)

    ioctx, err := pool.CreateStriper()
    assert.NoError(t, err)

    //start to upload
    file, err := os.Open(test_file_path)
    assert.NoError(t, err)


    ioctx.SetLayoutStripeUnit(512<<10)
    ioctx.SetLayoutObjectSize(4<<20)
    ioctx.SetLayoutStripeCount(4)


    writer := NewIoCtxWrapper("testoid", &ioctx)


    buf  := make([]byte,4<<20)
    n, err := io.CopyBuffer(writer, file, buf)

    assert.NoError(t, err)

    fmt.Printf("uploaded %d,%v\n", n, err)

    err = ioctx.Delete("testoid")

    assert.NoError(t, err)


    file.Close()

    err = conn.DeletePool(poolname)
    assert.NoError(t, err)

    conn.Shutdown()
}

func TestWriteSmallFile(t * testing.T) {
    conf_file_path := "/etc/ceph/ceph.conf"
    test_file_path := "./README.md"

    conn, _ := NewConn("admin")
    err := conn.ReadConfigFile(conf_file_path)
    assert.NoError(t, err)

    fmt.Println("connecting")
    err = conn.Connect()
    assert.NoError(t, err)


    poolname := GetUUID()
    err = conn.MakePool(poolname)
    assert.NoError(t, err)


    pool, err := conn.OpenPool(poolname)
    assert.NoError(t, err)

    data, err := ioutil.ReadFile(test_file_path)
    assert.NoError(t, err)

    err = pool.WriteSmallObject("testoid", data)
    assert.NoError(t, err)

    err = conn.DeletePool(poolname)
    assert.NoError(t, err)

    conn.Shutdown()
}
