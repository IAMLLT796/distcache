package geecache

type PeerPicker interface {
	// 用于根据传入的 key 选择相应节点 PeerGetter
	PickPeer(key string) (peer PeerGetter, ok bool)
}

type PeerGetter interface {
	// 从对应的 group 查找缓存值
	Get(group string, key string) ([]byte, error)
}
