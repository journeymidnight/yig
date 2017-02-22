all:
	go build
	go build tools/admin.go
	go build tools/delete.go
	go build tools/getrediskeys.go
	go build tools/lc.go
