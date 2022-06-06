package main

import (
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs_topic", // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")
	var idx int
	for {
		for _, topic := range []string{"student", "teacher"} {
			body := fmt.Sprintf("msg %d from %s", idx, topic)
			err = ch.Publish(
				"logs_topic", // exchange
				topic,        // routing key
				false,        // mandatory
				false,        // immediate
				amqp.Publishing{
					ContentType:  "text/plain",
					Body:         []byte(body),
					DeliveryMode: 2,
				})
			failOnError(err, "Failed to publish a message")
			log.Printf(" [x] Sent %s", body)
			idx += 1
			time.Sleep(time.Second)
		}
	}
}
