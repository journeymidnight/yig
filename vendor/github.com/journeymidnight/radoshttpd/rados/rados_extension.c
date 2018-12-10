#include <radosstriper/libradosstriper.h>
#include <rados/librados.h>
#include <errno.h>
#include <stdlib.h>

int try_break_lock(rados_ioctx_t io_ctx, rados_striper_t striper, char *oid){
        int exclusive;
        char tag[1024];
        char clients[1024];
        char cookies[1024];
        char addresses[1024];
        int ret = 0;
        size_t tag_len = 1024;
        size_t clients_len = 1024;
        size_t cookies_len = 1024;
        size_t addresses_len = 1024;
        char *firstObjOid = NULL;
        char tail[] = ".0000000000000000";
        firstObjOid = (char*)malloc(strlen(oid)+strlen(tail)+1);
        strcpy(firstObjOid, oid);
        strcat(firstObjOid, tail);
        ret = rados_list_lockers(io_ctx, firstObjOid, "striper.lock", &exclusive, tag, &tag_len, clients, &clients_len, cookies, &cookies_len, addresses, &addresses_len);
        if (ret < 0){
		goto out;
	}
	ret = rados_break_lock(io_ctx, firstObjOid, "striper.lock", clients, cookies);
	if (ret < 0){
		goto out;
	}
out:
	free(firstObjOid);
	return ret;
}


/* force delete the file */
int striprados_remove(rados_ioctx_t io_ctx, rados_striper_t striper, char *oid){
        int ret;
        int retry = 0;
retry:
        ret = rados_striper_remove(striper, oid);
        if (ret == -EBUSY && retry == 0){
                ret = try_break_lock(io_ctx,striper,oid);
                retry++;
                if (ret == 0)
                        goto retry;
        }
}


/*
 * 实际上在打完newobj patch以后，librados版本号应该+1, 但是为了和社区兼容，并不修改LIBRADOS_VER_EXTRA
 * 
 *                 |  run on new ceph   | run on old ceph
 *
--------------------------------------------------------------------
build on new ceph  |      good          |  test the NEWOBJ in runtime, than NO

------------------------------------------------------------

build on old ceph  |      NO            |  NO

----------------------------------------------------------

*/

int rados_write_with_newobj(rados_ioctx_t io_ctx, const char * oid, const char * buffer, size_t len) {
	
	/* if LIBRADOS_OP_FLAG_FADVISE_NEWOBJ is supported in OSD, good. Or OSD will ignore this flag */
        rados_write_op_t op = rados_create_write_op();
        rados_write_op_write(op, buffer, len, 0); 
        rados_write_op_set_flags(op, (1 << 31)); 
        int ret = rados_write_op_operate(op, io_ctx, oid, NULL, 0);
        rados_release_write_op(op);
        return ret;
}
