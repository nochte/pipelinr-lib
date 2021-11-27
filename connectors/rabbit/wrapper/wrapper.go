package wrapper

import (
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/streadway/amqp"
)

var wrappermux sync.Mutex

var wrappers map[string]*Wrapper

type Wrapper struct {
	channel *channel
	queue   string
}

func init() {
	wrappers = make(map[string]*Wrapper)
}

func Instance(url, queue string) *Wrapper {
	if w, ok := wrappers[url+queue]; ok {
		return w
	}
	wrappermux.Lock()
	defer wrappermux.Unlock()
	if w, ok := wrappers[url+queue]; ok {
		return w
	}
	wrappers[url+queue] = createInstance(url, queue)
	return wrappers[url+queue]
}

func createInstance(url, queue string) *Wrapper {
	channel := channelInstance(url, queue)
	w := &Wrapper{
		channel: channel,
		queue:   queue,
	}
	return w
}

func (w *Wrapper) UnsafePush(data amqp.Publishing) error {
	if w.channel.queue != w.queue {
		log.Infof("push mismatch %v / %v", w.channel.queue, w.queue)
	}
	return w.channel.getPushChannel().Publish(
		"",
		w.queue,
		false,
		false,
		data,
	)
}

func (w *Wrapper) Get() (amqp.Delivery, bool, error) {
	if w.channel.queue != w.queue {
		log.Infof("pull mismatch %v / %v", w.channel.queue, w.queue)
	}
	return w.channel.getReceiveChannel().Get(w.queue, true)
}

func (w *Wrapper) Close() error {
	return w.channel.close()
}

func (w *Wrapper) Inspect() (amqp.Queue, error) {
	return w.channel.getInspectChannel().QueueInspect(w.queue)
}
