package wrapper

import (
	"errors"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/streadway/amqp"
)

var chanmux sync.Mutex

// one channel per queue
var channels map[string]*channel

func init() {
	channels = make(map[string]*channel)
}

type channel struct {
	connection      *connection
	pushchannel     *amqp.Channel
	receivechannel  *amqp.Channel
	inspectchannel  *amqp.Channel
	queue           string
	isready         bool
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	notifyConnClose chan *amqp.Error
	done            chan bool
	deliveries      chan amqp.Delivery
}

func channelInstance(url string, queue string) *channel {
	if c, ok := channels[queue]; ok {
		return c
	}
	chanmux.Lock()
	defer chanmux.Unlock()
	if c, ok := channels[queue]; ok {
		return c
	}
	// channels[queue] = createChannel(createConnection(url), queue)
	channels[queue] = createChannel(connectionInstance(url), queue)
	return channels[queue]
}

func createChannel(c *connection, queue string) *channel {
	ch := &channel{
		connection: c,
		queue:      queue,
		deliveries: make(chan amqp.Delivery),
	}
	go ch.handleReconnect(c)
	c.attachChannel(ch)
	return ch
}

func (ch *channel) conClosed(er *amqp.Error) {
	ch.notifyConnClose <- er
}

func (ch *channel) connReady() bool {
	return ch.connection.isReady()
}

func (ch *channel) isReady() bool {
	return ch.connection.isReady() && ch.isready
}

const getChannelDelay = time.Second

func (ch *channel) getPushChannel() *amqp.Channel {
	for !ch.isReady() {
		log.Infof("channel not ready %v", ch.queue)
		time.Sleep(getChannelDelay)
	}
	return ch.pushchannel
}
func (ch *channel) getReceiveChannel() *amqp.Channel {
	for !ch.isReady() {
		log.Infof("channel not ready %v", ch.queue)
		time.Sleep(getChannelDelay)
	}
	return ch.receivechannel
}
func (ch *channel) getInspectChannel() *amqp.Channel {
	for !ch.isReady() {
		log.Infof("channel not ready %v", ch.queue)
		time.Sleep(getChannelDelay)
	}
	return ch.inspectchannel
}

func (ch *channel) handleReconnect(conn *connection) {
	for {
		ch.isready = false
		for !ch.connReady() {
			time.Sleep(reInitDelay)
		}
		pchh, er := conn.conn.Channel()
		if er != nil {
			log.Errorf("error on channel: %v", er)
			continue
		}
		rchh, er := conn.conn.Channel()
		if er != nil {
			log.Errorf("error on channel: %v", er)
			continue
		}
		ichh, er := conn.conn.Channel()
		if er != nil {
			log.Errorf("error on channel: %v", er)
			continue
		}
		ch.changeChannel(pchh, rchh, ichh)

		ch.isready = true
		// queuedeclare now
		_, er = ichh.QueueDeclare(
			ch.queue,
			true,
			false,
			false,
			false,
			amqp.Table{
				"x-max-priority": 10,
			},
		)
		if er != nil {
			log.Errorf("error declaring queue %v: %v", ch.queue, er)
			time.Sleep(reInitDelay)
			continue
		}
		// wait for conclose and reconnect
		select {
		case <-ch.done:
			return
		case err := <-ch.notifyConnClose:
			log.Warnf("connection closed, recreating channel: %v", err)
			continue
		case err := <-ch.notifyChanClose:
			log.Warnf("channel closed, recreating channel: %v", err)
			continue
		}
	}
}

func (ch *channel) changeChannel(pushchannel *amqp.Channel, receivechannel *amqp.Channel, inspectchannel *amqp.Channel) {
	log.Info("changeChannel")
	if ch.pushchannel != nil {
		ch.pushchannel.Close()
	}
	if ch.receivechannel != nil {
		ch.receivechannel.Close()
	}
	if ch.inspectchannel != nil {
		ch.inspectchannel.Close()
	}
	ch.notifyChanClose = make(chan *amqp.Error)
	ch.notifyConfirm = make(chan amqp.Confirmation, 5)
	ch.pushchannel = pushchannel
	ch.receivechannel = receivechannel
	ch.inspectchannel = inspectchannel
	ch.pushchannel.NotifyClose(ch.notifyChanClose)
	ch.pushchannel.NotifyPublish(ch.notifyConfirm)
	ch.receivechannel.NotifyClose(ch.notifyChanClose)
	ch.receivechannel.NotifyPublish(ch.notifyConfirm)
	ch.inspectchannel.NotifyClose(ch.notifyChanClose)
	ch.inspectchannel.NotifyPublish(ch.notifyConfirm)
}

func (ch *channel) close() error {
	if !ch.isready {
		return errors.New("channel already closed")
	}
	if er := ch.pushchannel.Close(); er != nil {
		return er
	}
	if er := ch.receivechannel.Close(); er != nil {
		return er
	}
	if er := ch.inspectchannel.Close(); er != nil {
		return er
	}
	close(ch.done)
	ch.isready = false
	return nil
}
