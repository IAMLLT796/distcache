package geecache

import (
	"distcache/geecache/consistenthash"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

/*
1.分布式缓存需要实现节点之间通信
2.建立基于 HTTP 的通信机制是比较常见和简单的做法
3.如果一个节点启动了 HTTP 服务，那么这个节点就可以被其他节点访问
*/

const (
	defaultBasePath = "/_geecache/"
	defaultReplicas = 50
)

// HTTPPool 实现了 PeerPicker 接口
type HTTPPool struct {
	// HTTP 节点之间的基础通信地址
	self        string
	basePath    string
	mu          sync.Mutex
	peers       *consistenthash.Map    // 根据具体的 key 选择节点
	httpGetters map[string]*httpGetter // 映射远程节点的 httpGetter, 每一个远程节点对应一个 httpGetter
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

type httpGetter struct {
	baseURL string
}

func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{baseURL: peer + p.basePath}
	}
}
func (p *HTTPPool) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s", peer)
		return p.httpGetters[peer], false
	}
	return nil, true
}

func (h *httpGetter) Get(group string, key string) ([]byte, error) {
	u := fmt.Sprintf("%v%v/%v", h.baseURL, url.QueryEscape(group), url.QueryEscape(key))
	res, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned: %v", res.Status)
	}

	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %v", err)
	}
	return bytes, nil
}

var _PeerGetter = (*httpGetter)(nil)
