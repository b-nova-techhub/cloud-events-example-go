package main

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/cloudevents/sdk-go/v2/protocol"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"log"
	"time"
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

		log.Printf("Button clicked: %t", data.Clicked)

		sendCloudEvent(event)
	} else if event.Type() == "com.bnova.techhub.get.activity" {
		log.Printf("Received event, %s", event)
		data := &ButtonEvent{}
		err := event.DataAs(data)
		if err != nil {
			log.Printf("failed to get data as ButtonEvent: %s", err)
		}

		if data.Clicked {
			log.Printf("Querying activity")
			result := sendCloudEvent(event)
			log.Printf("Result: %s", result)

			event.SetSource("cloud-events-example-go")
			if err := event.SetData(cloudevents.ApplicationJSON, result); err != nil {
				log.Fatalf("failed to set data, %v", err)
			}
			return &event, nil
		}
	} else {
		log.Printf("Unknown type, %s", event)

		return nil, cloudevents.NewHTTPResult(500, "Blöd gelaufen")
	}
	return nil, nil
}

func sendCloudEvent(event cloudevents.Event) *event.Event {
	c, err := cloudevents.NewClientHTTP()
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	event.SetSource("cloud-events-example-go")

	ctx := cloudevents.ContextWithTarget(cloudevents.WithEncodingStructured(context.Background()), "http://localhost:8081/")

	resp, result := c.Request(ctx, event)
	if cloudevents.IsUndelivered(result) {
		log.Printf("Failed to deliver request: %v", result)
	} else {
		log.Printf("Event delivered at %s, Acknowledged==%t ", time.Now(), cloudevents.IsACK(result))
		var httpResult *cehttp.Result
		if cloudevents.ResultAs(result, &httpResult) {
			log.Printf("Response status code %d", httpResult.StatusCode)
		}

		if resp != nil {
			log.Printf("Response, %s", resp)
			return resp
		} else {
			log.Printf("No response")
		}
	}
	return nil
}
