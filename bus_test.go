package evtbus_test

import (
	"sync"
	"testing"
	"time"

	"github.com/go-home-iot/event-bus"
	"github.com/stretchr/testify/require"
)

type MockEvent struct {
	Desc string
}

func (e *MockEvent) String() string {
	return e.Desc
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

func (c *MockConsumer) ConsumeEvent() evtbus.Event {
	return <-c.Channel
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

	e1 := &MockEvent{Desc: "e1"}
	e2 := &MockEvent{Desc: "e2"}

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

	e1 := &MockEvent{Desc: "e1"}
	e2 := &MockEvent{Desc: "e2"}

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

	e := &MockEvent{Desc: "e"}
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

	e1 := &MockEvent{Desc: "e1"}
	p.Produce(e1)

	// Take event of c2, but leave it on c1
	c2.ConsumeEvent()

	// Raise another event, c1 is full, but should not block
	// c2 should get event
	e2 := &MockEvent{Desc: "e2"}
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

	e := &MockEvent{Desc: "e"}
	p1.Produce(e)

	b.Stop()

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

func TestEnqueueFilter(t *testing.T) {
	b := evtbus.NewBus(10, 10)

	c := &MockConsumer{}
	b.AddConsumer(c)

	p := &MockProducer{}
	b.AddProducer(p)

	// Add a filter, first event should be blocked, next one should go through
	e1 := &MockEvent{Desc: "e1"}
	e2 := &MockEvent{Desc: "e2"}

	b.AddEnqueueFilter("myfilter", func(e evtbus.Event) bool {
		evt, ok := e.(*MockEvent)
		if !ok {
			return false
		}

		// Blocking the first event only
		if evt.Desc == "e1" {
			return true
		}
		return false
	}, 0)

	// Fire event and wait for it to be processed
	p.Produce(e1)
	time.Sleep(time.Millisecond * 500)

	// The consumer should not have received the event
	require.Equal(t, 0, len(c.Channel))

	// Next event should go through
	p.Produce(e2)
	time.Sleep(time.Millisecond * 500)

	require.Equal(t, 1, len(c.Channel))

	// Removing the filter should now let e1 go through
	b.RemoveEnqueueFilter("myfilter")

	// Fire event and wait for it to be processed
	p.Produce(e1)
	time.Sleep(time.Millisecond * 500)

	// The consumer should now have received the event
	require.Equal(t, 1, len(c.Channel))

}

func TestEnqueueFilterTimeout(t *testing.T) {
	b := evtbus.NewBus(10, 10)

	c := &MockConsumer{}
	b.AddConsumer(c)

	p := &MockProducer{}
	b.AddProducer(p)

	e1 := &MockEvent{Desc: "e1"}

	// The filter should last one second, then expire
	b.AddEnqueueFilter("myfilter", func(e evtbus.Event) bool {
		evt, ok := e.(*MockEvent)
		if !ok {
			return false
		}

		// Blocking the first event only
		if evt.Desc == "e1" {
			return true
		}
		return false
	}, 1*time.Second)

	// Fire event and wait for it to be processed
	err := p.Produce(e1)
	require.Nil(t, err)
	time.Sleep(time.Millisecond * 500)

	// The consumer should not have received the event
	require.Equal(t, 0, len(c.Channel))

	// Wait at least one second , then try again, filter should have expired
	time.Sleep(time.Second * 2)

	// Next event should go through
	err = p.Produce(e1)
	require.Nil(t, err)
	time.Sleep(time.Millisecond * 500)

	require.Equal(t, 1, len(c.Channel))
}
