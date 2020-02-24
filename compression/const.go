package compression

var (
	// TODO need more content-type
	ContentTypeNeedCompress = []string{"text/plain","text/html","text/asp","text/css","text/csv"," 	text/x-component",
	"text/webviewhtml","text/plain","text/x-vcard","text/scriptlet","java/*"}
	// TODO need more suffix
	SuffixNeedCompress = []string{".txt",".bat",".c",".bas",".prg",".cmd",".log"}
)
