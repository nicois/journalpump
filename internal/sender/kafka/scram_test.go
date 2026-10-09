// Copyright 2026, Aiven, https://aiven.io
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package kafka

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

// TestSCRAMConfigValidates: sarama rejects a SCRAM-SHA-256/512 config
// that lacks a SCRAMClientGeneratorFunc, so the resolved sarama config
// must validate for both mechanisms (review item B5).
func TestSCRAMConfigValidates(t *testing.T) {
	for _, mechanism := range []string{"SCRAM-SHA-256", "SCRAM-SHA-512"} {
		t.Run(mechanism, func(t *testing.T) {
			raw := baseConfig()
			raw["security_protocol"] = "SASL_PLAINTEXT"
			raw["sasl_mechanism"] = mechanism
			raw["sasl_plain_username"] = "user"
			raw["sasl_plain_password"] = "password"
			pc, err := resolveProducerConfig(raw)
			if err != nil {
				t.Fatalf("resolveProducerConfig: %v", err)
			}
			cfg, err := pc.saramaConfig()
			if err != nil {
				t.Fatalf("saramaConfig: %v", err)
			}
			if cfg.Net.SASL.SCRAMClientGeneratorFunc == nil {
				t.Fatal("SCRAMClientGeneratorFunc is nil; sarama would reject the config")
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
		})
	}
}

// TestSCRAMClientAuthenticates drives a full SCRAM exchange between the
// generated sarama client and an xdg-go/scram server, proving the client
// actually authenticates (item B5).
func TestSCRAMClientAuthenticates(t *testing.T) {
	const (
		user     = "user"
		password = "password"
	)
	cases := []struct {
		name string
		hgf  scram.HashGeneratorFcn
		gen  func() sarama.SCRAMClient
	}{
		{"SHA256", scram.SHA256, scramClientGenerator(scram.SHA256)},
		{"SHA512", scram.SHA512, scramClientGenerator(scram.SHA512)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Server-side stored credentials for the same password.
			client, err := tc.hgf.NewClient(user, password, "")
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			stored := client.GetStoredCredentials(scram.KeyFactors{Salt: "somesalt", Iters: 4096})
			server, err := tc.hgf.NewServer(func(string) (scram.StoredCredentials, error) {
				return stored, nil
			})
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			serverConv := server.NewConversation()

			sc := tc.gen()
			if err := sc.Begin(user, password, ""); err != nil {
				t.Fatalf("Begin: %v", err)
			}
			// client-first
			clientFirst, err := sc.Step("")
			if err != nil {
				t.Fatalf("client first step: %v", err)
			}
			serverFirst, err := serverConv.Step(clientFirst)
			if err != nil {
				t.Fatalf("server first step: %v", err)
			}
			clientFinal, err := sc.Step(serverFirst)
			if err != nil {
				t.Fatalf("client final step: %v", err)
			}
			serverFinal, err := serverConv.Step(clientFinal)
			if err != nil {
				t.Fatalf("server final step: %v", err)
			}
			if _, err := sc.Step(serverFinal); err != nil {
				t.Fatalf("client validate server step: %v", err)
			}
			if !sc.Done() {
				t.Error("client conversation not done after server final message")
			}
			if !serverConv.Valid() {
				t.Error("server did not validate the client")
			}
		})
	}
}
