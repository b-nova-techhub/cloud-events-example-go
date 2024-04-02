package main

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/protocol"
	"log"
)

type ButtonEvent struct {
	Clicked bool `json:"clicked"`
}

func main() {
	ctx := context.Background()
	p, err := cloudevents.NewHTTP()
	if err != nil {
		log.Fatalf("failed to create protocol: %s", err.Error())
	}

	c, err := cloudevents.NewClient(p)
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	log.Printf("will listen on :8080\n")
	log.Fatalf("failed to start receiver: %s", c.StartReceiver(ctx, receive))
}

func receive(ctx context.Context, event cloudevents.Event) (*event.Event, protocol.Result) {
	if event.Type() == "com.bnova.techhub.button.clicked" {
		log.Printf("Received event, %s", event)
		data := &ButtonEvent{}
		err := event.DataAs(data)
		if err != nil {
			log.Printf("failed to get data as ButtonEvent: %s", err)
		}

		log.Println(data.Clicked)

		sendCloudEvent(data)
	} else {
		log.Printf("Unknown type, %s", event)

		return nil, cloudevents.NewHTTPResult(500, "Blöd gelaufen")
	}
	return nil, nil
}

func sendCloudEvent(data *ButtonEvent) {
	c, err := cloudevents.NewClientHTTP()
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	ce := cloudevents.NewEvent()
	ce.SetSource("cloud-events-example-go")
	ce.SetType("com.bnova.techhub.button.clicked")
	if err := ce.SetData(cloudevents.ApplicationJSON, data); err != nil {
		log.Fatalf("failed to set data, %v", err)
	}
	ce.SetID("abc")

	ctx := cloudevents.ContextWithTarget(context.Background(), "http://localhost:8081/")

	if result := c.Send(ctx, ce); cloudevents.IsUndelivered(result) {
		log.Fatalf("failed to send, %v", result)
	} else {
		log.Printf("sent: %v", ce)
		log.Printf("result: %v", result)
	}
}
