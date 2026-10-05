// Package kafka wraps franz-go with the topic layout, producer and consumer
// the services share.
package kafka

import "github.com/SpaghettiRebel/Notification-Service/internal/notification"

// InvalidDLQTopic receives messages that cannot be decoded or validated.
const InvalidDLQTopic = "notification.invalid.dlq"

// TopicFor returns the topic of a delivery channel, e.g. notification.tg.
func TopicFor(channel string) string { return "notification." + channel }

// DLQTopicFor returns the dead-letter topic of a channel topic, e.g. notification.tg.dlq.
func DLQTopicFor(topic string) string { return topic + ".dlq" }

// Topics returns the channel topics the worker consumes.
func Topics() []string {
	topics := make([]string, 0, len(notification.Channels))
	for _, channel := range notification.Channels {
		topics = append(topics, TopicFor(channel))
	}
	return topics
}
