// Copyright 2026, Aiven, https://aiven.io
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

// scramClient adapts xdg-go/scram to sarama's SCRAMClient interface.
// sarama requires a SCRAMClientGeneratorFunc whenever the mechanism is
// SCRAM-SHA-256/512 and rejects the config before connecting without one,
// so the sender must supply a working client (review item B5).
type scramClient struct {
	hashGenerator scram.HashGeneratorFcn
	client        *scram.Client
	conversation  *scram.ClientConversation
}

func (c *scramClient) Begin(userName, password, authzID string) error {
	client, err := c.hashGenerator.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	c.client = client
	c.conversation = client.NewConversation()
	return nil
}

func (c *scramClient) Step(challenge string) (string, error) {
	return c.conversation.Step(challenge)
}

func (c *scramClient) Done() bool {
	return c.conversation.Done()
}

func scramClientGenerator(hashGenerator scram.HashGeneratorFcn) func() sarama.SCRAMClient {
	return func() sarama.SCRAMClient {
		return &scramClient{hashGenerator: hashGenerator}
	}
}
