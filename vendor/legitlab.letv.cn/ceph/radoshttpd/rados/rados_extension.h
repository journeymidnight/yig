#ifndef RADOS_EXTENSION_H
#define RADOS_EXTENSION_H

/* force delete the file */
int striprados_remove(rados_ioctx_t ioctx, rados_striper_t striper, char *oid);

int rados_write_with_newobj(rados_ioctx_t io_ctx, const char * oid, const char * buffer, size_t len);

#endif
