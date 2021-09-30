package rabbitmq

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/streadway/amqp"
	"gotest.tools/assert"
)

var localConnection = Connection{
	protocol: CONNECTION_PROTOCOL_AMQP,
	account:  "guest",
	password: "guest",
	host:     "localhost",
	port:     5672,
}

var defaultExclusiveQueue = Queue{
	amqp.Queue{},
	"",    // name
	false, // durable
	false, // delete when unused
	true,  // exclusive
	false, // no-wait
	nil,   // arguments
	"",    //routing key
}

func TestFanout(t *testing.T) {
	var consumeCountMutex *sync.Mutex = &sync.Mutex{}
	var consumeCount int
	fanoutExchange := Exchange{
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	}
	consume := Consume{
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
		func(d amqp.Delivery, args ...interface{}) {
			mutex, _ := args[0].(*sync.Mutex)
			mutex.Lock()
			defer mutex.Unlock()
			consumeCount++
		},
		[]interface{}{consumeCountMutex},
	}
	consumeMQ, _ := CreateMQ(MQ_TYPE_CONSUME, localConnection)
	publishMQ, _ := CreateMQ(MQ_TYPE_PUBLISH, localConnection)
	consumeMQ.CreateConsumer(defaultExclusiveQueue, fanoutExchange, consume)
	consumeMQ.CreateConsumer(defaultExclusiveQueue, fanoutExchange, consume)
	publishMQ.CreateProducer(defaultExclusiveQueue, fanoutExchange)

	go func() {
		for i := 0; i < 1000; i++ {
			for _, unit := range publishMQ.units {
				unit.GetChannel().Publish(
					fanoutExchange.name, // exchange
					"",                  // routing key
					false,               // mandatory
					false,               // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte("test"),
					})
			}
		}
	}()

	time.Sleep(time.Second * 3)

	assert.Equal(t, consumeCount, 2000)
}

func TestDirect(t *testing.T) {
	directExchange := Exchange{
		"logs_direct", // name
		"direct",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	}
	consumeMQ, _ := CreateMQ(MQ_TYPE_CONSUME, localConnection)
	publishMQ, _ := CreateMQ(MQ_TYPE_PUBLISH, localConnection)
	consumeRouting1 := false
	consumeRouting2 := false
	consumeMQ.CreateConsumer(Queue{
		amqp.Queue{},
		"",       // name
		false,    // durable
		true,     // delete when unused
		true,     // exclusive
		false,    // no-wait
		nil,      // arguments
		"test_1", //routing key
	}, directExchange, Consume{
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
		func(d amqp.Delivery, args ...interface{}) {
			consumeRouting1 = true
		},
		nil,
	})
	consumeMQ.CreateConsumer(Queue{
		amqp.Queue{},
		"",       // name
		false,    // durable
		true,     // delete when unused
		true,     // exclusive
		false,    // no-wait
		nil,      // arguments
		"test_2", //routing key
	}, directExchange, Consume{
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
		func(d amqp.Delivery, args ...interface{}) {
			consumeRouting2 = true
		},
		nil,
	})
	publishMQ.CreateProducer(Queue{amqp.Queue{},
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
		"",    //routing key
	}, directExchange)

	go func() {
		for i := 1; i < 3; i++ {
			for _, unit := range publishMQ.units {
				unit.GetChannel().Publish(
					"logs_direct",             // exchange
					fmt.Sprintf("test_%d", i), // routing key
					false,                     // mandatory
					false,                     // immediate
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte("test"),
					})
			}
		}
	}()

	time.Sleep(time.Second * 3)

	assert.Equal(t, consumeRouting1, true)
	assert.Equal(t, consumeRouting2, true)
}

func TestConsumeRecconect(t *testing.T) {
	reconnectTestQueue := Queue{
		amqp.Queue{},
		"reconnect_test", // name
		false,            // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		nil,              // arguments
		"test_1",         //routing key
	}
	exhange := Exchange{
		"logs_direct", // name
		"direct",      // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // no-wait
		nil,           // arguments
	}
	consumeMQ, _ := CreateMQ(MQ_TYPE_CONSUME, localConnection)
	publishMQ, _ := CreateMQ(MQ_TYPE_PUBLISH, localConnection)
	var consumeCountMutex *sync.Mutex = &sync.Mutex{}
	consumeCountMap := map[string]bool{}
	consumeMQ.CreateConsumer(reconnectTestQueue, exhange, Consume{
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
		func(d amqp.Delivery, args ...interface{}) {
			mutex, _ := args[0].(*sync.Mutex)
			mutex.Lock()
			defer mutex.Unlock()
			consumeCountMap[string(d.Body)] = true
			if err := d.Ack(false); err != nil {
				fmt.Println(err.Error())
			}
		},
		[]interface{}{consumeCountMutex},
	})

	publishMQ.CreateProducer(reconnectTestQueue, exhange)

	go func() {
		for t := 0; t < 5; t++ {
			for i := 0; i < 50000; i++ {
				for _, unit := range publishMQ.units {
					unit.GetChannel().Publish(
						"logs_direct", // exchange
						"test_1",      // routing key
						false,         // mandatory
						false,         // immediate
						amqp.Publishing{
							ContentType: "text/plain",
							Body:        []byte(strconv.Itoa(50000*t + i)),
						})
				}
			}
			time.Sleep(5 * time.Second)
		}
	}()

	wait := make(chan bool)
	go func() {
		time.Sleep(time.Minute)
		t.Error("timeout")
		wait <- false
	}()
	go func() {
		for {
			time.Sleep(5 * time.Second)
			if len(consumeCountMap) >= 250000 {
				wait <- true
				break
			}
		}
	}()
	<-wait

	assert.Equal(t, len(consumeCountMap), 250000)
}

func TestCreateMQ(t *testing.T) {
	type args struct {
		t          MQType
		connection Connection
	}
	tests := []struct {
		name    string
		args    args
		want    *MQ
		wantErr bool
	}{
		{
			name: "simple",
			args: args{
				t: MQ_TYPE_CONSUME,
				connection: Connection{
					protocol: CONNECTION_PROTOCOL_AMQP,
					account:  "guest",
					password: "guest",
					host:     "localhost",
					port:     5672,
				},
			},
			want:    &MQ{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CreateMQ(tt.args.t, tt.args.connection)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateMQ() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
