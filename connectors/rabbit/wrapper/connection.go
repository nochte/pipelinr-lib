package wrapper

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/streadway/amqp"
)

var conmux sync.Mutex

var connections map[string]*connection

func init() {
	connections = make(map[string]*connection)
}

type connection struct {
	url              string
	conn             *amqp.Connection
	isready          bool
	done             chan bool
	notifyConnClose  chan *amqp.Error
	attachedChannels []*channel
	mux              sync.Mutex
}

func connectionInstance(url string) *connection {
	if c, ok := connections[url]; ok {
		return c
	}
	conmux.Lock()
	defer conmux.Unlock()
	if c, ok := connections[url]; ok {
		return c
	}
	connections[url] = createConnection(url)
	return connections[url]
}

func createConnection(url string) *connection {
	con := &connection{
		url:              url,
		attachedChannels: make([]*channel, 0, 1),
		mux:              sync.Mutex{},
	}
	go con.handleReconnect(url)
	return con
}

func (c *connection) attachChannel(ch *channel) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.attachedChannels = append(c.attachedChannels, ch)
}

const reconnectDelay = time.Second * 5

func (c *connection) handleReconnect(addr string) {
	for {
		c.isready = false

		// get the connection
		con, er := c.connect(addr)
		// couldn't connect
		if er != nil {
			time.Sleep(reconnectDelay)
			log.Warnf("couldn't reconnect, trying again")
			continue
		}
		// no error, connection is handled
		//  block on handleReInit until quit signal
		if quit := c.handleReInit(con); quit {
			break
		}
	}
}

func (c *connection) connect(addr string) (*amqp.Connection, error) {
	log.Info("connecting")
	conn, err := amqp.Dial(addr)

	if err != nil {
		log.Errorf("error connecting: %v", err)
		return nil, err
	}

	c.changeConnection(conn)
	log.Info("connected")
	return conn, nil
}

const reInitDelay = time.Second * 2

// returns true if it's quit time
// returns false if reconnect
// blocks until quit or reconnect
func (c *connection) handleReInit(conn *amqp.Connection) bool {
	log.Info("handleReInit loop start")
	for {
		log.Info("handleReInit loop looping")
		c.isready = false
		er := c.init(conn)
		if er != nil {
			time.Sleep(reInitDelay)
			log.Warnf("failed to initialize connection, retrying %v", er)
			continue
		}

		log.Info("blocking til something happens")
		select {
		case <-c.done:
			log.Info("got done signal")
			return true
		case er := <-c.notifyConnClose:
			log.Errorf("connection closed with error, reconnecting (%v)\n", er)
			for _, ch := range c.attachedChannels {
				go ch.conClosed(er)
			}
			return false
		}
	}
	return false
}

func (c *connection) init(conn *amqp.Connection) error {
	log.Info("init")
	c.isready = true
	return nil
}

// wires up the new connection
func (c *connection) changeConnection(conn *amqp.Connection) {
	if c.conn != nil {
		log.Info("closing previous connection")
		c.conn.Close()
	}
	c.conn = conn
	c.notifyConnClose = make(chan *amqp.Error)
	c.conn.NotifyClose(c.notifyConnClose)
}

func (c *connection) isReady() bool {
	return c.isready
}
