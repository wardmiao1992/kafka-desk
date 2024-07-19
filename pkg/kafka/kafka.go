package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/PaesslerAG/gval"
	"github.com/PaesslerAG/jsonpath"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Host string
	Port string
}

type Client struct {
	config     *Config
	connection *kafka.Conn
}

func NewClient(config *Config) (*Client, error) {
	conn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%s", config.Host, config.Port))
	if err != nil {
		return nil, err
	}
	controller, err := conn.Controller()
	if err != nil {
		return nil, err
	}
	var connLeader *kafka.Conn
	connLeader, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return nil, err
	}
	return &Client{
		connection: connLeader,
		config:     config,
	}, nil
}

func (client *Client) ListTopics() ([]string, error) {
	partitions, err := client.connection.ReadPartitions()
	if err != nil {
		return nil, err
	}

	m := map[string]struct{}{}
	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}
	topics := make([]string, 0)
	for k := range m {
		topics = append(topics, k)
	}
	return topics, nil
}

func (client *Client) SearchMessage(topic string, jsonPath string, start time.Time, end time.Time) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var messageFilter *gval.Evaluable
	if jsonPath != "" {
		builder := gval.Full(jsonpath.PlaceholderExtension())

		path, err := builder.NewEvaluable(jsonPath)
		if err != nil {
			return nil, err
		}
		messageFilter = &path
	}

	brokers := []string{fmt.Sprintf("%s:%s", client.config.Host, client.config.Port)}
	batchSize := int(10e6) // 10MB

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		MaxBytes: batchSize,
		GroupID:  "kafka-desktop",
	})
	defer reader.Close()

	reader.SetOffsetAt(ctx, start)
	messages := make([]string, 0)
	for {
		m, err := reader.FetchMessage(ctx)

		if err != nil {
			if strings.Contains(err.Error(), "context deadline exceeded") {
				return messages, nil
			}
			return nil, err
		}
		if m.Time.After(end) {
			break
		}

		if messageFilter != nil {
			v := interface{}(nil)
			if err = json.Unmarshal(m.Value, &v); err != nil {
				return nil, err
			}
			//messageFilter(ctx, v)
		}
		var compactMsg bytes.Buffer
		err = json.Compact(&compactMsg, m.Value)
		if err != nil {
			return nil, err
		}
		messages = append(messages, compactMsg.String())
	}
	return messages, nil
}

func (client *Client) WriteMessage(topic, message string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w := &kafka.Writer{
		Addr:  kafka.TCP(fmt.Sprintf("%s:%s", client.config.Host, client.config.Port)),
		Topic: topic,
	}
	w.WriteMessages(ctx)
	w.AllowAutoTopicCreation = true
	id := generateID()
	m := kafka.Message{Key: []byte(id), Value: []byte(message)}
	if err := w.WriteMessages(ctx, m); err != nil {
		return err
	}
	return nil
}

func (client *Client) DeleteTopic(topic string) error {
	return client.connection.DeleteTopics(topic)
}

func generateID() string {
	id := uuid.New()
	return strings.Replace(id.String(), "-", "", -1)
}
