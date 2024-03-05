package geecache

import (
	"fmt"
	"log"
	"net/http"
	"strings"
)

/*
1.分布式缓存需要实现节点之间通信
2.建立基于 HTTP 的通信机制是比较常见和简单的做法
3.如果一个节点启动了 HTTP 服务，那么这个节点就可以被其他节点访问
*/

const defaultBasePath = "/_geecache/"

// HTTPPool 实现了 PeerPicker 接口
type HTTPPool struct {
	// HTTP 节点之间的基础通信地址
	self     string
	basePath string
}

// 构造函数
func NewHTTPPool(self string) *HTTPPool {
	return &HTTPPool{
		self:     self,            // 用来记录自己的地址，包括主机名/IP 和端口
		basePath: defaultBasePath, // 作为节点间通讯地址的前缀，默认是 /_distcache/
	}
}

// Log info with server name
func (p *HTTPPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

// ServerHTTP handle all http requests
func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.Log("%s, %s", r.Method, r.URL.Path)
	// /<basepath>/<groupname>/<key> required
	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	groupName := parts[0]
	key := parts[1]

	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}

	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(view.ByteSlice())
}
