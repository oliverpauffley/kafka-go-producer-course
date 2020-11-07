package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/segmentio/kafka-go"
)

func main() {
	twitterKey, ok := os.LookupEnv("TWITTER_KEY")
	if !ok {
		log.Fatal("must set env var 'TWITTER_KEY'")
	}
	twitterSecret, ok := os.LookupEnv("TWITTER_SECRET")
	if !ok {
		log.Fatal("must set env var 'TWITTER_SECRET'")
	}
	twitterToken, ok := os.LookupEnv("TWITTER_TOKEN")
	if !ok {
		log.Fatal("must set env var 'TWITTER_TOKEN'")
	}
	twitterTokenSecret, ok := os.LookupEnv("TWITTER_TOKEN_SECRET")
	if !ok {
		log.Fatal("must set env var 'TWITTER_TOKEN_SECRET'")
	}

	kafkaAdddress, ok := os.LookupEnv("KAFKA_BROKER_ADDRESS")
	if !ok {
		log.Println("kafka broker address no set, using default localhost:9092")
		kafkaAdddress = "localhost:9092"
	}
	kafkaTopic, ok := os.LookupEnv("KAFKA_TOPIC")
	if !ok {
		log.Fatal("must set env var 'KAFKA_TOPIC'")
	}

	twitterFilter, ok := os.LookupEnv("TWITTER_FILTER")
	if !ok {
		log.Fatal("must set env var 'TWITTER_FILTER'")
	}

	twitterClient := NewTwitterClient(twitterKey, twitterSecret, twitterToken, twitterTokenSecret)

	stream, err := NewTwitterStream(twitterClient, twitterFilter)
	if err != nil {
		log.Fatalf("error creating twitter stream, error: %s", err)
	}

	kafkaWriter, err := NewKafkaProducer(kafkaAdddress, kafkaTopic)
	if err != nil {
		log.Fatalf("error creating kafka producer, error: %s", err)
	}

	// demux handles different twitter messages in different ways.
	// currently we only care about tweets
	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {

		jsonTweet, err := json.Marshal(tweet)
		if err != nil {
			log.Fatal(err)
		}

		log.Println(tweet.IDStr)
		log.Println(tweet.Text)

		err = kafkaWriter.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(tweet.IDStr),
				Value: []byte(jsonTweet),
			},
		)
		if err != nil {
			log.Fatal(err)
		}
	}

	// listen for new stream messages
	go demux.HandleChan(stream.Messages)

	// on signal interupt shutdown
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	// close twitter stream first then kafka to flush all messages.
	stream.Stop()
	kafkaWriter.Close()

}
