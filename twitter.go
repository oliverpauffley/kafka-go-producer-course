package main

import (
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
)

// NewTwitterClient returns a new twitter api client.
func NewTwitterClient(twitterKey, twitterSecret, accessToken, accessSecret string) *twitter.Client {
	// create a twitter client
	config := oauth1.NewConfig(twitterKey, twitterSecret)
	token := oauth1.NewToken(accessToken, accessSecret)

	httpClient := config.Client(oauth1.NoContext, token)
	return twitter.NewClient(httpClient)
}

// NewTwitterStream returns a message queue of twitter messages from given filter(s).
func NewTwitterStream(client *twitter.Client, filter ...string) (*twitter.Stream, error) {
	params := &twitter.StreamFilterParams{
		Track:         filter,
		StallWarnings: twitter.Bool(true),
	}
	stream, err := client.Streams.Filter(params)
	if err != nil {
		return nil, err
	}
	return stream, nil
}
