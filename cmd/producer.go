package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	producer_utils "sample-producer/producer"

	"github.com/pkg/errors"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/cobra"
)

func CreateProducerCmd() (*cobra.Command, error) {
	var filePath string
	var kafkaServers string
	var kafkaUsername string
	var kafkaPassword string
	var kafkaSecurityProtocol string
	var kafkaSASKMechanism string
	var sslCaLocation string
	var sslKeyPassword string
	var sslCertLocation string
	var sslKeyLocation string
	var topic string
	var numMessages int
	var ordered bool
	var startNum int

	command := cobra.Command{
		Use: "produce",
		Run: func(cmd *cobra.Command, args []string) {
			
			kafkaProducerConfig := producer_utils.Config{
				BootstrapServers:          kafkaServers,
				SecurityProtocol:          kafkaSecurityProtocol,
				SASLMechanism:             kafkaSASKMechanism,
				SASLUsername:              kafkaUsername,
				SASLPassword:              kafkaPassword,
				ReadTimeoutSeconds:        0,
				GroupId:                   "",
				QueueBufferingMaxMessages: 0,
				QueuedMaxMessagesKbytes:   0,
				FetchMessageMaxBytes:      0,
				SSLCALocation:             sslCaLocation,
				SSLKeyLocation:            sslKeyLocation,
				SSLCertLocation:           sslCertLocation,
				SSLKeyPassword:            sslKeyPassword,
				EnableAutoOffsetStore:     false,
			}
			producer, err := producer_utils.NewProducer(kafkaProducerConfig)
			if err != nil {
				panic(errors.Wrap(err, "Unable to create producer"))
			}
			queueBufferingMaxMessages := producer_utils.DefaultQueueBufferingMaxMessages
			if kafkaProducerConfig.QueueBufferingMaxMessages > 0 {
				queueBufferingMaxMessages = kafkaProducerConfig.QueueBufferingMaxMessages
			}
			deliveryChan := make(chan kafka.Event, queueBufferingMaxMessages)
			go func() { // Tricky: kafka require specific deliveryChan to use Flush function
				for e := range deliveryChan {
					m := e.(*kafka.Message)
					if m.TopicPartition.Error != nil {
						panic(fmt.Sprintf("Failed to deliver message: %v\n", m.TopicPartition))
					} else {
						fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}
				}
			}()
			if topic == "" {
				panic(errors.Wrap(fmt.Errorf("no topic defined"), "Error parsing the topic"))
			}
			messages := producer_utils.GenerateMessages(topic, numMessages, startNum, ordered)
			
			err = Run(producer, deliveryChan, messages)
			if err != nil {
				panic(errors.Wrap(err, "Error while running importer"))
			}
		},
	}
	command.Flags().StringVarP(&filePath, "file", "f", "", "Output file path (required)")
	command.Flags().StringVar(&kafkaServers, "kafka-servers", "", "Kafka servers string")
	command.Flags().StringVar(&kafkaUsername, "kafka-username", "", "Kafka username")
	command.Flags().StringVar(&kafkaPassword, "kafka-password", "", "Kafka password")
	command.Flags().StringVar(&kafkaSASKMechanism, "kafka-sasl-mechanism", "", "Kafka password")
	command.Flags().StringVar(&kafkaSecurityProtocol, "kafka-security-protocol", "", "Kafka security protocol")
	command.MarkFlagsRequiredTogether("kafka-username", "kafka-password", "kafka-sasl-mechanism", "kafka-security-protocol")
	command.Flags().StringVar(&sslCaLocation, "ssl-ca-location", "", "location of client ca cert file in pem")
	command.Flags().StringVar(&sslKeyPassword, "ssl-key-password", "", "password for ssl private key passphrase")
	command.Flags().StringVar(&sslCertLocation, "ssl-certificate-location", "", "client's certificate location")
	command.Flags().StringVar(&sslKeyLocation, "ssl-key-location", "", "path to ssl private key")
	command.Flags().StringVar(&topic, "topic", "", "topic to which to produce the messages to")
	command.Flags().IntVar(&numMessages, "num-messages", 1, "number of messages to produce to the topic to")
	command.Flags().BoolVarP(&ordered, "ordered", "i", false, "when true msgs will contain key-n...key-n+1 and value-n...value-n+1")
	command.Flags().IntVar(&startNum, "start-num", 0, "when chosen ordered this can define the message number from which it will produce next")
	return &command, nil
}

func Run(producer *kafka.Producer, deliveryChan chan kafka.Event, messages []kafka.Message) error {
	cx := make(chan os.Signal, 1)
	signal.Notify(cx, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-cx
		producer.Flush(30 * 1000)
		os.Exit(1)
	}()
	defer func() {
		producer.Flush(30 * 1000)
	}()

	for _, message := range messages {
		err := producer.Produce(&message, deliveryChan)
		if err != nil {
			return errors.Wrapf(err, "Failed to produce message: %s", string(message.Value))
		}
	}
	return nil
}
