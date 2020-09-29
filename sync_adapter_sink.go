package substrate

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/uw-labs/sync/rungroup"
)

var (
	// ErrSinkAlreadyClosed is an error returned when user tries to publish a message or
	// close a sink after it was already closed.
	ErrSinkAlreadyClosed = errors.New("sink was closed already")
	// ErrSinkClosedOrFailedDuringSend is an error returned when a sink is closed or fails while sending a message.
	ErrSinkClosedOrFailedDuringSend = errors.New("sink was closed or failed while sending the message")
)

// NewSynchronousMessageSink returns a new synchronous message sink, given an
// AsyncMessageSink.  When Close is called on the SynchronousMessageSink, this
// is also propogated to the underlying SynchronousMessageSink
func NewSynchronousMessageSink(ams AsyncMessageSink) SynchronousMessageSink {
	spa := &synchronousMessageSinkAdapter{
		aprod: ams,

		closeReq:  make(chan struct{}),
		closed:    make(chan struct{}),
		closeErr:  make(chan error, 1),
		toProduce: make(chan *produceReq),
	}
	go spa.loop()
	return spa
}

type synchronousMessageSinkAdapter struct {
	aprod AsyncMessageSink

	closeReq chan struct{}
	closed   chan struct{} // is used to signal to publishers that the sink was closed
	closeErr chan error    // stores error from the backend or from closing it

	toProduce chan *produceReq
}

type produceReq struct {
	m    Message
	done chan error
	ctx  context.Context
}

func (spa *synchronousMessageSinkAdapter) loop() {
	toSend := make(chan Message)
	acks := make(chan Message)

	rg, ctx := rungroup.New(context.Background())

	rg.Go(func() error {
		return spa.aprod.PublishMessages(ctx, acks, toSend)
	})

	rg.Go(func() error {
		seq := 0
		needAcks := make(map[int]*produceReq)
		defer func() {
			println("rg2 in defer func")
			// Send error to all waiting publish requests before shutting down
			for seq, req := range needAcks {
				select {
				case <-req.ctx.Done():
					println("rg2 defer: context done")
				case req.done <- ErrSinkClosedOrFailedDuringSend:
					println("rg2 defer: sending ErrSinkClosedOrFailed to req.done of msg", seq)
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				println("rg2: context done 1")
				return nil
			case pr := <-spa.toProduce:
				seq++
				needAcks[seq] = pr
				select {
				case <-ctx.Done():
					println("rg2: context done 2")
					return nil
				case toSend <- seqMessage{seq: seq, Message: pr.m}:
					println("rg2: sent message", seq)
				case <-spa.closeReq:
					println("rg2: got closereq 2")
					return nil
				}
			case ack := <-acks:
				println("rg2: got ack")
				msg, ok := ack.(seqMessage)
				if !ok {
					panic(fmt.Sprintf("unexpected message: %s", ack))
				}
				req, ok := needAcks[msg.seq]
				if !ok {
					panic(fmt.Sprintf("unexpected sequence: %v", msg.seq))
				}
				if msg.Message != req.m {
					panic(fmt.Sprintf("wrong message expected: %s got: %s", req.m, msg.Message))
				}
				close(req.done)
				delete(needAcks, msg.seq)
			case <-spa.closeReq:
				println("rg2: got closereq 1")
				return nil
			}
		}
	})

	// Wait for sink and loop to terminate and send close error tp closed channel
	if sinkErr := rg.Wait(); sinkErr == nil || sinkErr == context.Canceled {
		errmsg := "<nil>"
		if sinkErr != nil {
			errmsg = sinkErr.Error()
		}
		println("rg.Wait() done 1, sinkErr:", errmsg)
		spa.closeErr <- spa.aprod.Close()
	} else {
		println("rg.Wait() done 2, sinkErr:", sinkErr.Error())
		if err := spa.aprod.Close(); err != nil {
			println("rg.Wait() done close err:", err.Error())
			spa.closeErr <- errors.Errorf("sink error: %v sink close error: %v", sinkErr, err)
		} else {
			spa.closeErr <- sinkErr
		}
	}
	close(spa.closed)
	close(spa.closeErr)
}

func (spa *synchronousMessageSinkAdapter) Close() error {
	println("smsa.Close() called")
	select {
	case err, ok := <-spa.closeErr:
		if ok {
			return err
		}
		return ErrSinkAlreadyClosed
	case spa.closeReq <- struct{}{}:
		// Check if channel is still open in case Close
		// is called concurrently more than once
		err, ok := <-spa.closeErr
		if ok {
			return err
		}
		return ErrSinkAlreadyClosed
	}
}

func (spa *synchronousMessageSinkAdapter) PublishMessage(ctx context.Context, m Message) error {
	pr := &produceReq{m, make(chan error), ctx}

	select {
	case spa.toProduce <- pr:
		select {
		case err := <-pr.done:
			println("PM: produceReq.done err:", err)
			return err
		case <-ctx.Done():
			println("PM: context done")
			return ctx.Err()
		}
	case <-spa.closed:
		println("PM: closed was closed")
		return ErrSinkAlreadyClosed
	case <-ctx.Done():
		println("PM: context done")
		return ctx.Err()
	}
}

func (spa *synchronousMessageSinkAdapter) Status() (*Status, error) {
	return spa.aprod.Status()
}

type seqMessage struct {
	seq int
	Message
}

func (msg seqMessage) Original() Message {
	return msg.Message
}
