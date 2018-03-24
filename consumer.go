package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

var rabbitUser = flag.String("user", "guest", "rabbit user")
var rabbitPassword = flag.String("password", "guest", "rabbit password")
var rabbitHost = flag.String("host", "localhost", "rabbit host")
var rabbitPort = flag.String("port", "5672", "rabbit port")
var queueName = flag.String("queue-name", "", "queue name")
var maxRetry = flag.Int("max-retry", 3, "Max connection retry before panic")

/*type Message struct {
	id             int
	lastHasResults int
	sighup         bool
}*/

// Rabbit : rabbit struct
type Rabbit struct {
	id int
	ch *amqp.Channel
	q  amqp.Queue
	d  <-chan amqp.Delivery
}

func (r *Rabbit) setChannel(ch *amqp.Channel) {
	r.ch = ch
}

func (r *Rabbit) setQueue(q amqp.Queue) {
	r.q = q
}

func (r *Rabbit) setConsume() {
	var err error
	r.d, err = r.ch.Consume(
		r.q.Name, // queue
		"",       // consumer
		true,     // auto-ack
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)

	failAndLog(err, "Impossibile dichiarare consumer")
}

func (r *Rabbit) closeChannel() {
	r.ch.Close()
}

func getConnection(connectionName string) *amqp.Connection {
	retry := 0
	for {
		log.Printf("Tentativo connessione a %s\n", connectionName)
		conn, err := amqp.Dial(connectionName)
		if err == nil {
			log.Printf("[+] Connesso")
			return conn
		}

		retry++

		if retry >= *maxRetry {
			panic("[-] Numero massimo di retry raggiunto - CRASH!")
		}

		time.Sleep(time.Second * (time.Duration(retry * 5)))
	}
}

func getChannel(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	failAndLog(err, "Impossibile aprire il canale")

	return ch
}

func getQueue(ch *amqp.Channel, name string) amqp.Queue {
	q, err := ch.QueueDeclare(
		name,  // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failAndLog(err, "Impossibile dichiarare la coda")

	return q
}

func failAndLog(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func read(r Rabbit) {
	for d := range r.d {
		log.Printf("Received a message: %s from %d", d.Body, r.id)
	}
}

func main() {
	flag.Parse()
	if *queueName == "" {
		panic("Missing required field queue-name")
	}
	connectionName := fmt.Sprintf("amqp://%s:%s@%s:%s", *rabbitUser, *rabbitPassword, *rabbitHost, *rabbitPort)

	var rTotal = 3

	for {
		conn := getConnection(connectionName)

		rb := make([]Rabbit, rTotal)

		for i := range rb {
			rb[i].id = i
			rb[i].setChannel(getChannel(conn))
			rb[i].setQueue(getQueue(rb[i].ch, *queueName))
			rb[i].setConsume()
		}

		for i := range rb {
			go read(rb[i])
		}

		for {

		}

		for i := range rb {
			rb[i].closeChannel()
		}

		conn.Close()
	}
}
