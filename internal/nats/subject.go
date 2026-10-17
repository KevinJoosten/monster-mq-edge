package nats

import "strings"

// SubjectToTopic converts a NATS subject or filter to an MQTT topic or filter:
// . -> /, * -> +, > -> #. Character-wise and global (NC:492-494).
func SubjectToTopic(subject string) string {
	return strings.NewReplacer(".", "/", "*", "+", ">", "#").Replace(subject)
}

// TopicToSubject converts an MQTT topic or filter to a NATS subject or filter:
// / -> ., + -> *, # -> >, space -> _ (NC:502-504).
func TopicToSubject(topic string) string {
	return strings.NewReplacer("/", ".", "+", "*", "#", ">", " ", "_").Replace(topic)
}

// topicMatchesFilter checks if a concrete MQTT topic matches an MQTT topic filter,
// using main's matcher (NC:509-523). Note that "a/#" matches "a".
func topicMatchesFilter(topic, filter string) bool {
	if filter == "#" {
		return true
	}
	topicParts := strings.Split(topic, "/")
	filterParts := strings.Split(filter, "/")
	for i, fp := range filterParts {
		if fp == "#" {
			return true
		}
		if i >= len(topicParts) {
			return false
		}
		if fp != "+" && fp != topicParts[i] {
			return false
		}
	}
	return len(topicParts) == len(filterParts)
}
