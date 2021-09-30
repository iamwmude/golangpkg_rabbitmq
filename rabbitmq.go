package rabbitmq

import (
	"fmt"
	"log"
	"time"

	"github.com/lithammer/shortuuid"
	"github.com/streadway/amqp"
)

type MQType int
type ConnectionProtocol string

const (
	CONNECTION_RECONNECT_INTERVAL time.Duration      = 3 * time.Second
	MQ_TYPE_PUBLISH               MQType             = 1
	MQ_TYPE_CONSUME               MQType             = 2
	CONNECTION_PROTOCOL_AMQP      ConnectionProtocol = "amqp"
	CONNECTION_PROTOCOL_AMQPS     ConnectionProtocol = "amqps"
)

// Use separate connections to publish and consume
type MQ struct {
	id         string
	mqType     MQType // publish or consume
	connection Connection
	units      map[string]IUnit
}

type Connection struct {
	connection *amqp.Connection
	protocol   ConnectionProtocol
	account    string
	password   string
	host       string
	port       int
}

type IUnit interface {
	GetID() string
	setChannel(*amqp.Channel)
	GetChannel() *amqp.Channel
	setQueueQueue(amqp.Queue)
	GetQueue() Queue
	GetExchange() Exchange
}

type Unit struct {
	id       string
	channel  *amqp.Channel
	queue    Queue
	exchange Exchange
}

type Consumer struct {
	Unit
	Consume
}
type Consume struct {
	consumer     string
	autoAck      bool
	exclusive    bool
	noLocal      bool
	noWait       bool
	args         amqp.Table
	callback     func(amqp.Delivery, ...interface{})
	callbackArgs []interface{}
}

type Producer struct {
	Unit
}

type Queue struct {
	queue      amqp.Queue
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
	args       amqp.Table
	routingKey string
}

type Exchange struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
	args       amqp.Table
}

func CreateMQ(t MQType, connection Connection) (*MQ, error) {
	mq := &MQ{
		id:         shortuuid.New(),
		mqType:     t,
		connection: connection,
		units:      map[string]IUnit{},
	}

	if err := mq.connect(); err != nil {
		return nil, err
	}

	mq.registerConnectionReconnect()

	return mq, nil
}

func (mq *MQ) connect() (err error) {
	mq.connection.connection, err = amqp.Dial(fmt.Sprintf("%s://%s:%s@%s:%d/", mq.connection.protocol, mq.connection.account, mq.connection.password, mq.connection.host, mq.connection.port))
	if err != nil {
		return err
	}

	return nil
}
func (mq *MQ) reconnect() (err error) {
	for _, unit := range mq.units {
		if err = mq.initUnit(unit); err != nil {
			return err
		}
	}

	return nil
}
func (mq *MQ) registerConnectionReconnect() {
	go func() {
		for {
			err := <-mq.connection.connection.NotifyClose(make(chan *amqp.Error))
			log.Printf("[%v] Connection closed. Reason: %s\n", mq.mqType, err.Reason)

			for {
				var err error
				if err = mq.connect(); err == nil {
					if err = mq.reconnect(); err == nil {
						log.Printf("Connection reconnected.")
						break
					}
				}

				log.Printf("Reconnect failed. Reason: %s", err.Error())
				time.Sleep(CONNECTION_RECONNECT_INTERVAL)
			}
		}
	}()
}
func (mq *MQ) GetID() string {
	return mq.id
}
func (mq *MQ) GetType() MQType {
	return mq.mqType
}
func (mq *MQ) GetConnection() Connection {
	return mq.connection
}

func (u *Unit) GetID() string {
	return u.id
}
func (u *Unit) setChannel(channel *amqp.Channel) {
	u.channel = channel
}
func (u *Unit) GetChannel() *amqp.Channel {
	return u.channel
}
func (u *Unit) setQueueQueue(queue amqp.Queue) {
	u.queue.queue = queue
}
func (u *Unit) GetQueue() Queue {
	return u.queue
}
func (u *Unit) GetExchange() Exchange {
	return u.exchange
}
func (mq *MQ) initUnit(unit IUnit) (err error) {
	channel, err := mq.connection.connection.Channel()
	if err != nil {
		return err
	}

	unit.setChannel(channel)

	if err := channel.ExchangeDeclare(unit.GetExchange().name, unit.GetExchange().kind, unit.GetExchange().durable, unit.GetExchange().autoDelete, unit.GetExchange().internal, unit.GetExchange().noWait, unit.GetExchange().args); err != nil {
		return fmt.Errorf("error in declaring the exchange %s", err)
	}

	queue, err := channel.QueueDeclare(unit.GetQueue().name, unit.GetQueue().durable, unit.GetQueue().autoDelete, unit.GetQueue().exclusive, unit.GetQueue().noWait, unit.GetQueue().args)
	if err != nil {
		return fmt.Errorf("error in declaring the queue %s", err)
	}

	unit.setQueueQueue(queue)

	if err := channel.QueueBind(unit.GetQueue().name, unit.GetQueue().routingKey, unit.GetExchange().name, unit.GetQueue().noWait, unit.GetQueue().args); err != nil {
		return fmt.Errorf("Queue Bind error: %s", err)
	}

	if mq.GetType() == MQ_TYPE_CONSUME {
		consume, ok := unit.(*Consumer)
		if !ok {
			return fmt.Errorf("covert type to consumer failed")
		}
		if msgs, err := unit.GetChannel().Consume(
			unit.GetQueue().name, // queue
			consume.consumer,     // consumer
			consume.autoAck,      // auto-ack
			consume.exclusive,    // exclusive
			consume.noLocal,      // no-local
			consume.noWait,       // no-wait
			consume.args,         // args
		); err != nil {
			return fmt.Errorf("error in consume %s", err)
		} else {
			go func() {
				for d := range msgs {
					consume.callback(d, consume.callbackArgs...)
				}
			}()
		}
	}

	return nil
}

func (mq *MQ) CreateConsumer(queue Queue, exchange Exchange, consume Consume) (err error) {
	unit := &Consumer{
		Unit: Unit{
			id:       shortuuid.New(),
			queue:    queue,
			exchange: exchange,
		},
		Consume: consume,
	}

	if err := mq.initUnit(unit); err != nil {
		return err
	}

	mq.units[unit.GetID()] = unit

	return nil
}

func (mq *MQ) CreateProducer(queue Queue, exchange Exchange) (err error) {
	unit := &Producer{
		Unit: Unit{
			id:       shortuuid.New(),
			queue:    queue,
			exchange: exchange,
		},
	}

	if err := mq.initUnit(unit); err != nil {
		return err
	}

	mq.units[unit.GetID()] = unit

	return nil
}
