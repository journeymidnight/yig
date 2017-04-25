package rados
/* vim: set ts=4 shiftwidth=4 smarttab noet : */

// #cgo LDFLAGS: -lrados
// #include <stdlib.h>
// #include <rados/librados.h>
// #include <radosstriper/libradosstriper.h>
// #include "rados_extension.h"
import "C"

import "unsafe"

type StriperPool struct {
    striper C.rados_striper_t
    ioctx   C.rados_ioctx_t
}

type AioCompletion struct {
    completion C.rados_completion_t
}

func (sp *StriperPool) Read(oid string, data []byte, offset uint64) (int, error) {
  if len(data) == 0 {
        return 0,nil
  }

  c_oid := C.CString(oid)
  defer C.free(unsafe.Pointer(c_oid))

  ret := C.rados_striper_read(sp.striper, c_oid,
                               (*C.char)(unsafe.Pointer(&data[0])),
                               C.size_t(len(data)),
                               C.uint64_t(offset))
  if ret >= 0 {
        return int(ret), nil
  } else {
    return 0, RadosError(int(ret))
  }

}

func (sp *StriperPool) Destroy() {
    C.rados_striper_destroy(sp.striper);
}

func (sp *StriperPool) State(oid string) (uint64,uint64, error) {
    c_oid := C.CString(oid)
    defer C.free(unsafe.Pointer(c_oid))
    var c_psize C.uint64_t
    var c_ptime C.time_t
    ret := C.rados_striper_stat(sp.striper, c_oid, &c_psize, &c_ptime)
    if ret < 0 {
      return 0, 0, RadosError(int(ret))
    }
    return uint64(c_psize), uint64(c_ptime), nil
}

func (sp *StriperPool) Truncate(oid string, offset uint64) error{
    c_oid := C.CString(oid)
    defer C.free(unsafe.Pointer(c_oid))
	ret := C.rados_striper_trunc(sp.ioctx, c_oid, C.uint64_t(offset))
	if ret < 0 {
		return RadosError(int(ret))
	}
	return nil
}

func (sp *StriperPool) Delete(oid string) error {
    c_oid := C.CString(oid)
    defer C.free(unsafe.Pointer(c_oid))
    // ret := C.rados_striper_remove(sp.striper, c_oid) use force remove now
    ret := C.striprados_remove(sp.ioctx, sp.striper, c_oid);
    if ret < 0 {
      return RadosError(int(ret))
    }
    return nil
}

func (sp *StriperPool) Write(oid string, data []byte, offset uint64) (int, error) {
  if len(data) == 0 {
        return 0,nil
  }

  c_oid := C.CString(oid)
  defer C.free(unsafe.Pointer(c_oid))

  ret := C.rados_striper_write(sp.striper, c_oid,
                               (*C.char)(unsafe.Pointer(&data[0])),
                               C.size_t(len(data)),
                               C.uint64_t(offset))
  if ret >= 0 {
        return int(ret), nil
  } else {
    return 0, RadosError(int(ret))
  }

}


func (sp *StriperPool) WriteAIO(c *AioCompletion, oid string, data []byte, offset uint64) (int, error) {
	if len(data) == 0 {
		return 0,nil
	}

	c_oid := C.CString(oid)
	defer C.free(unsafe.Pointer(c_oid))

	ret := C.rados_striper_aio_write(sp.striper, c_oid, c.completion, (*C.char)(unsafe.Pointer(&data[0])),  C.size_t(len(data)), C.uint64_t(offset))
	if ret >= 0 {
		return int(ret), nil
	} else {
		return 0, RadosError(int(ret))
	}

}

func (sp *StriperPool) Flush() {
	C.rados_striper_aio_flush(sp.striper)
}

func (sp * StriperPool) SetLayoutStripeUnit(stripe_unit uint ) int {
	return int(C.rados_striper_set_object_layout_stripe_unit(sp.striper, C.uint(stripe_unit)))
}

func (sp * StriperPool) SetLayoutStripeCount(stripe_count uint) int {
	return int(C.rados_striper_set_object_layout_stripe_count(sp.striper, C.uint(stripe_count)))

}

func (sp * StriperPool) SetLayoutObjectSize(object_size uint) int{
	return int(C.rados_striper_set_object_layout_object_size(sp.striper, C.uint(object_size)))

}

func (c *AioCompletion) Create() error {
	ret := C.rados_aio_create_completion(nil, nil, nil, (*C.rados_completion_t)(&c.completion))
	if ret >= 0 {
		return nil
	} else {
		return RadosError(int(ret))
	}
}

func (c *AioCompletion) WaitForComplete() {
	C.rados_aio_wait_for_complete(c.completion)
}

func (c *AioCompletion) Release() {
	C.rados_aio_release(c.completion)
}

func (c *AioCompletion) IsComplete() int{
	ret := int(C.rados_aio_is_complete(c.completion))
	return ret
}

func (c *AioCompletion) GetReturnValue() int {
	ret := int(C.rados_aio_get_return_value(c.completion))
	return ret
}

