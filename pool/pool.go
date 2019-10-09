package pool

import "errors"

var (
	ErrClosed = errors.New("pool is closed")
)

//线程池接口
type Pool interface {
	Get() (interface{}, error)

	Put(interface{}) error

	Close(interface{}) error

	Release()

	Len() int
}
