package main

import (
	"fmt"
	"database/sql"
	"log"
	"github.com/streadway/amqp"
	_ "github.com/lib/pq"
)
var db *sql.DB

func init() {
	var err error
	db, err = sql.Open("postgres", "postgres://sudhakar:palaniM@67@localhost/log?sslmode=disable")
	if err != nil {
		panic(err)
	}

	if err = db.Ping(); err != nil {
		panic(err)
	}
	fmt.Println("You connected to your database.")
}


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

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)
		go func() {
			for d := range msgs {
				log.Printf("Received a message: %s", d.Body)
				_, err = db.Exec("INSERT INTO messages (text) VALUES ($1)", d.Body)
				failOnError(err, "Failed to insert a data")
			}
		}()

		log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
