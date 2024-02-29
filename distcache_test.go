package distcache

import (
	"reflect"
	"testing"
)

func TestGetter(t *testing.T) {
	var f Getter = GetterFunc(func(key string) ([]byte, error) {
		return []byte(key), nil
	})

	except := []byte("key")

	if v, _ := f.Get("key"); !reflect.DeepEqual(v, except) {
	}
	t.Errorf("callback failed")
}
