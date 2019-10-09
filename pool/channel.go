package pool

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type Config struct {
	InitialCap  int                         //最小连接数
	MaxCap      int                         //最大连接数
	Factory     func() (interface{}, error) //生成连接方法
	Close       func(interface{}) error     //关闭连接方法
	Ping        func(interface{}) error     //检查连接是否有效的方法
	IdleTimeout time.Duration               //连接最大空闲时间 超过该时间则将失效
}

//存放连接信息
type channelPool struct {
	mu          sync.Mutex
	conns       chan *idleConn
	factory     func() (interface{}, error)
	close       func(interface{}) error
	ping        func(interface{}) error
	idleTimeout time.Duration
}

type idleConn struct {
	conn interface{}
	t    time.Time
}

//初始化连接
func NewChannelPool(config *Config) (Pool, error) {
	if config.InitialCap < 0 || config.MaxCap <= 0 || config.InitialCap > config.MaxCap {
		return nil, errors.New("invalid capacity settings")
	}

	if config.Factory == nil {
		return nil, errors.New("invalid factory func settings")
	}

	if config.Close == nil {
		return nil, errors.New("invalid close func settings")
	}

	c := &channelPool{
		conns:       make(chan *idleConn, config.MaxCap),
		factory:     config.Factory,
		close:       config.Close,
		idleTimeout: config.IdleTimeout,
	}

	if config.Ping != nil {
		c.ping = config.Ping
	}

	for i := 0; i < config.InitialCap; i++ {
		conn, err := c.factory()
		if err != nil {
			c.Release()
			return nil, fmt.Errorf("factory is not able to fill the pool:%s", err)
		}
		c.conns <- &idleConn{conn: conn, t: time.Now()}
	}
	return c, nil
}

//释放连接池
func (c *channelPool) Release() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.ping = nil
	closeFun := c.close
	c.close = nil
	c.mu.Unlock()
	if conns == nil {
		return
	}
	close(conns)
	for wrapConn := range conns {
		closeFun(wrapConn.conn)
	}

}

//获取连接数
func (c *channelPool) Len() int {
	return len(c.getConns())
}

//关闭连接
func (c *channelPool) Close(conn interface{}) error {
	if conn == nil {
		return errors.New("connection is nil,reject")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.close == nil {
		return nil
	}
	return c.close(conn)
}

//检查连接是否有效
func (c *channelPool) Ping(conn interface{}) error {
	if conn == nil {
		return errors.New("connection is nil,reject")
	}
	return c.ping(conn)
}

//获取连接
func (c *channelPool) getConns() chan *idleConn {
	c.mu.Lock()
	conns := c.conns
	c.mu.Unlock()
	return conns
}

//新增连接
func (c *channelPool) Put(conn interface{}) error {
	if conn == nil {
		return errors.New("connection is nil,reject")
	}
	c.mu.Lock()
	if c.conns == nil {
		c.mu.Unlock()
		return c.Close(conn)
	}
	select {
	case c.conns <- &idleConn{conn: conn, t: time.Now()}:
		c.mu.Unlock()
		return nil
	default:
		c.mu.Unlock()
		return c.Close(conn)
	}
}

func (c *channelPool) Get() (interface{}, error) {
	conns := c.getConns()
	if conns == nil {
		return nil, ErrClosed
	}
	for {
		select {
		case wrapConn := <-conns:
			if wrapConn == nil {
				return nil, ErrClosed
			}

			if timeout := c.idleTimeout; timeout > 0 {
				if wrapConn.t.Add(timeout).Before(time.Now()) {
					c.Close(wrapConn.conn)
					continue
				}
			}

			if c.ping != nil {
				if err := c.Ping(wrapConn.conn); err != nil {
					fmt.Println("conn is not able to be connected:", err)
					continue
				}
			}
			return wrapConn.conn, nil
		default:
			c.mu.Lock()
			if c.factory == nil {
				c.mu.Unlock()
				continue
			}
			conn, err := c.factory()
			c.mu.Unlock()
			if err != nil {
				return nil, err
			}
			return conn, nil
		}
	}
}
