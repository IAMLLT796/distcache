package geecache

import (
	"fmt"
	"log"
	"reflect"
	"testing"
)

func TestGetter(t *testing.T) {
	// 借助 GetterFunc 的类型转换，将一个匿名函数转换成了接口 f Getter
	var f Getter = GetterFunc(func(key string) ([]byte, error) {
		return []byte(key), nil
	})

	except := []byte("key")

	if v, _ := f.Get("key"); !reflect.DeepEqual(v, except) {
		t.Fatal("callback failed")
	}
}

// 使用一个 map 模拟耗时的数据库
var db0 = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

// 创建 group 实例
func TestGet(t *testing.T) {
	loadCounts := make(map[string]int, len(db0))
	dist := NewGroup("scores", 2<<10, GetterFunc(
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] search key", key)
			if v, ok := db0[key]; ok {
				if _, ok := loadCounts[key]; !ok {
					loadCounts[key] = 0
				}
				loadCounts[key] += 1
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))
	for k, v := range db0 {
		if view, err := dist.Get(k); err != nil || view.String() != v {
			t.Fatal("failed to get value of Tom")
		}
		if _, err := dist.Get(k); err != nil || loadCounts[k] > 1 {
			t.Fatalf("cache %s miss", k)
		}
	}
	if view, err := dist.Get("unknown"); err == nil {
		t.Fatalf("the value of unknown should be empty, but %s got", view)
	}
}
