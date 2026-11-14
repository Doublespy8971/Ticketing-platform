package consumer

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
)

type MockMessageReader struct {
	mu           sync.Mutex
	messages     []kafka.Message
	committed    []kafka.Message
	commitErrors map[int64]error
}

func NewMockMessageReader(msgs []kafka.Message) *MockMessageReader {
	return &MockMessageReader{
		messages:     msgs,
		commitErrors: make(map[int64]error),
	}
}

func (m *MockMessageReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.messages) == 0 {
		return kafka.Message{}, context.Canceled
	}
	msg := m.messages[0]
	m.messages = m.messages[1:]
	return msg, nil
}

func (m *MockMessageReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, msg := range msgs {
		if err, ok := m.commitErrors[msg.Offset]; ok {
			return err
		}
		m.committed = append(m.committed, msg)
	}
	return nil
}

func (m *MockMessageReader) CommittedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.committed)
}

func (m *MockMessageReader) Close() error {
	return nil
}
