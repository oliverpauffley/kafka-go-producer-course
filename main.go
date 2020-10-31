package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
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

	twitterClient := NewTwitterClient(twitterKey, twitterSecret, twitterToken, twitterTokenSecret)

	stream, err := NewTwitterStream(twitterClient, "kafka")
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
		log.Println(tweet)

		err := kafkaWriter.WriteMessages(context.Background(),
			kafka.Message{
				Key:   []byte(tweet.User.ScreenName),
				Value: []byte(tweet.Text),
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

func NewTwitterClient(twitterKey, twitterSecret, accessToken, accessSecret string) *twitter.Client {
	// create a twitter client
	config := oauth1.NewConfig(twitterKey, twitterSecret)
	token := oauth1.NewToken(accessToken, accessSecret)

	httpClient := config.Client(oauth1.NoContext, token)
	return twitter.NewClient(httpClient)
}

func NewTwitterStream(client *twitter.Client, filter string) (*twitter.Stream, error) {
	params := &twitter.StreamFilterParams{
		Track:         []string{filter},
		StallWarnings: twitter.Bool(true),
	}
	stream, err := client.Streams.Filter(params)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func NewKafkaProducer(kafkaAddress, topic string) (*kafka.Writer, error) {
	conn, err := kafka.Dial("tcp", kafkaAddress)
	if err != nil {
		return nil, err
	}

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     6,
		ReplicationFactor: 1,
	})
	if err != nil {
		return nil, err
	}

	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{kafkaAddress},
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: 1,
	}), nil
}
