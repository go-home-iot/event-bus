package evtbus_test

import (
	"sync"
	"testing"

	"github.com/go-home-iot/event-bus"
	"github.com/stretchr/testify/require"
)

type MockEvent struct {
}

type MockConsumer struct {
	Name                string
	StartConsumingCount int
	StopConsumingCount  int
	Channel             chan evtbus.Event
}

func (c *MockConsumer) ConsumerName() string {
	if c.Name != "" {
		return c.Name
	}
	return "MockConsumer"
}

func (c *MockConsumer) StartConsuming(ch chan evtbus.Event) {
	c.Channel = ch
	c.StartConsumingCount++
}

func (c *MockConsumer) ConsumeEvent() {
	<-c.Channel
}

func (c *MockConsumer) StopConsuming() {
	c.StopConsumingCount++
}

type MockProducer struct {
	StartProducingCount int
	StopProducingCount  int
	Bus                 *evtbus.Bus
}

func (p *MockProducer) Produce(e *MockEvent) error {
	return p.Bus.Enqueue(e)
}

func (p *MockProducer) ProducerName() string {
	return "MockProducer"
}

func (p *MockProducer) StartProducing(b *evtbus.Bus) {
	p.StartProducingCount++
	p.Bus = b
}

func (p *MockProducer) StopProducing() {
	p.StopProducingCount++
}

func TestAddProducer(t *testing.T) {
	b := evtbus.NewBus(100, 100)

	p := &MockProducer{}
	b.AddProducer(p)

	require.Equal(t, 1, p.StartProducingCount)
	require.Equal(t, 0, p.StopProducingCount)
}

func TestFullBusReturnsError(t *testing.T) {
	b := evtbus.NewBus(1, 100)

	p := &MockProducer{}
	b.AddProducer(p)

	e1 := &MockEvent{}
	e2 := &MockEvent{}

	// Simulate the producer, producing events, the first one should
	// be enqueued, the second should fail, since we set the bus capacity to 1
	err := p.Produce(e1)
	require.Nil(t, err)

	err = p.Produce(e2)
	require.Equal(t, evtbus.ErrBusFull, err)
}

func TestAddConsumer(t *testing.T) {
	b := evtbus.NewBus(10, 10)

	c := &MockConsumer{}
	b.AddConsumer(c)

	require.Equal(t, 1, c.StartConsumingCount)
}

func TestMultipleProducers(t *testing.T) {
	b := evtbus.NewBus(10, 10)

	p1 := &MockProducer{}
	p2 := &MockProducer{}

	b.AddProducer(p1)
	b.AddProducer(p2)

	c := &MockConsumer{}
	b.AddConsumer(c)

	e1 := &MockEvent{}
	e2 := &MockEvent{}

	p1.Produce(e1)
	p2.Produce(e2)

	<-c.Channel
	<-c.Channel
}

func TestConsumersReceiveEvent(t *testing.T) {
	b := evtbus.NewBus(10, 10)

	p := &MockProducer{}
	c1 := &MockConsumer{}
	c2 := &MockConsumer{}

	b.AddProducer(p)
	b.AddConsumer(c1)
	b.AddConsumer(c2)

	var wg sync.WaitGroup
	wg.Add(2)

	e := &MockEvent{}
	var evt1 evtbus.Event
	var evt2 evtbus.Event
	go func() {
		evt1 = <-c1.Channel
		wg.Done()
	}()
	go func() {
		evt2 = <-c2.Channel
		wg.Done()
	}()

	p.Produce(e)

	// Wait until both consumers have received the event
	wg.Wait()

	require.Equal(t, e, evt1)
	require.Equal(t, e, evt2)
}

func TestBlockedConsumerDoesntBlockOtherConsumers(t *testing.T) {
	// If a consumers queue is full, shouldn't affect other consumers
	// from receiving events without being blocked

	b := evtbus.NewBus(10, 1)

	p := &MockProducer{}

	c1 := &MockConsumer{Name: "c1"}
	c2 := &MockConsumer{Name: "c2"}

	b.AddProducer(p)
	b.AddConsumer(c1)
	b.AddConsumer(c2)

	e1 := &MockEvent{}
	p.Produce(e1)

	// Take event of c2, but leave it on c1
	c2.ConsumeEvent()

	// Raise another event, c1 is full, but should not block
	// c2 should get event
	e2 := &MockEvent{}
	p.Produce(e2)

	evt := <-c2.Channel
	require.Equal(t, e2, evt)
}

func TestStopCallsAllProducersAndConsumers(t *testing.T) {
	b := evtbus.NewBus(10, 10)

	p1 := &MockProducer{}
	p2 := &MockProducer{}

	c1 := &MockConsumer{}
	c2 := &MockConsumer{}

	b.AddProducer(p1)
	b.AddProducer(p2)
	b.AddConsumer(c1)
	b.AddConsumer(c2)

	b.Stop()

	e := &MockEvent{}
	p1.Produce(e)

	// After stopping, should be nothing on the channel
	// and it should be closed
	e1, more1 := <-c1.Channel
	e2, more2 := <-c2.Channel

	require.Nil(t, e1)
	require.Nil(t, e2)
	require.False(t, more1)
	require.False(t, more2)

	require.Equal(t, 1, c1.StopConsumingCount)
	require.Equal(t, 1, c2.StopConsumingCount)
	require.Equal(t, 1, p1.StopProducingCount)
	require.Equal(t, 1, p2.StopProducingCount)
}

func TestRemoveProducer(t *testing.T) {
	b := evtbus.NewBus(10, 10)

	p := &MockProducer{}
	b.AddProducer(p)

	b.RemoveProducer(p)
	require.Equal(t, 1, p.StopProducingCount)
}

func TestRemoveConsumer(t *testing.T) {
	b := evtbus.NewBus(10, 10)

	c := &MockConsumer{}
	b.AddConsumer(c)

	// Stop consuming should be called and also the channel the consumer
	// receives events on should be closed
	b.RemoveConsumer(c)
	require.Equal(t, 1, c.StopConsumingCount)
	_, more := <-c.Channel
	require.False(t, more)
}