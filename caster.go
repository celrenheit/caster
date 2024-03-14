/*
 * @Author: guiguan
 * @Date:   2019-09-19T00:53:54+10:00
 * @Last modified by:   guiguan
 * @Last modified time: 2019-11-04T16:17:09+11:00
 */

// Package caster implements a dead simple and performant message broadcaster (pubsub) library
package caster

import (
	"context"
)

type operator int

const (
	opPub operator = iota
	opTryPub
	opSub
	opUnsub
	opClose
)

type operation[T any] struct {
	operator operator
	operand  operandInfo[T]
	okCh     chan bool
}

type operandInfo[T any] struct {
	pub   T
	sub   subInfo[T]
	unSub chan T
}

type subInfo[T any] struct {
	ch  chan T
	ctx context.Context
}

// Caster represents a message broadcaster
type Caster[T any] struct {
	done chan struct{}
	op   chan operation[T]
}

// New creates a new caster
func New[T any](ctx context.Context) *Caster[T] {
	if ctx == nil {
		ctx = context.Background()
	}

	c := &Caster[T]{
		done: make(chan struct{}),
		op:   make(chan operation[T]),
	}

	go func() {
		subs := map[chan T]context.Context{}

		checkCtx := func(sCh chan T, sCtx context.Context) bool {
			select {
			case <-sCtx.Done():
				delete(subs, sCh)
				close(sCh)
				return false
			default:
				return true
			}
		}

	topLoop:
		for {
			select {
			case <-ctx.Done():
				break topLoop
			case o := <-c.op:
				switch o.operator {
				case opPub:
					for sCh, sCtx := range subs {
						if !checkCtx(sCh, sCtx) {
							continue
						}

						select {
						case sCh <- o.operand.pub:
						}
					}
				case opTryPub:
					var (
						l    = len(subs)
						sent int
					)
					for sCh, sCtx := range subs {
						if !checkCtx(sCh, sCtx) {
							continue
						}

						select {
						case sCh <- o.operand.pub:
							sent++
						default:
						}
					}

					if o.okCh != nil {
						o.okCh <- sent == l
					}
				case opSub:
					sIn := o.operand.sub
					subs[sIn.ch] = sIn.ctx
				case opUnsub:
					sCh := o.operand.unSub
					delete(subs, sCh)
					close(sCh)
				case opClose:
					break topLoop
				}
			}
		}

		for sCh := range subs {
			close(sCh)
		}

		close(c.done)
	}()

	return c
}

// Done returns a done channel that is closed when current caster is closed
func (c *Caster[T]) Done() <-chan struct{} {
	return c.done
}

// Close closes current caster and all subscriber channels. Ok value indicates whether the operation
// is performed or not. When current caster is closed, it stops receiving further operations and the
// operation won't be performed.
func (c *Caster[T]) Close() (ok bool) {
	select {
	case <-c.done:
		return false
	case c.op <- operation[T]{
		operator: opClose,
	}:
		return true
	}
}

// Sub subscribes to current caster and returns a new channel with the given buffer for the
// subscriber to receive the broadcasting message. When the given ctx is canceled, current caster
// will unsubscribe the subscriber channel and close it. Ok value indicates whether the operation is
// performed or not. When current caster is closed, it stops receiving further operations and the
// operation won't be performed. A closed receiver channel will be returned if ok is false.
func (c *Caster[T]) Sub(ctx context.Context, capacity uint) (sCh chan T, ok bool) {
	if ctx == nil {
		ctx = context.Background()
	}

	sCh = make(chan T, capacity)

	select {
	case <-ctx.Done():
		close(sCh)
	case <-c.done:
		close(sCh)
	case c.op <- operation[T]{
		operator: opSub,
		operand: operandInfo[T]{
			sub: subInfo[T]{
				ch:  sCh,
				ctx: ctx,
			},
		},
	}:
	}

	ok = true
	return
}

// Unsub unsubscribes the given subscriber channel from current caster and closes it. Ok value
// indicates whether the operation is performed or not. When current caster is closed, it stops
// receiving further operations and the operation won't be performed.
func (c *Caster[T]) Unsub(subCh chan T) (ok bool) {
	select {
	case <-c.done:
		return false
	case c.op <- operation[T]{
		operator: opUnsub,
		operand: operandInfo[T]{
			unSub: subCh,
		},
	}:
		return true
	}
}

// Pub publishes the given message to current caster, so the caster in turn broadcasts the message
// to all subscriber channels. Ok value indicates whether the operation is performed or not. When
// current caster is closed, it stops receiving further operations and the operation won't be
// performed.
func (c *Caster[T]) Pub(msg T) (ok bool) {
	select {
	case <-c.done:
		return false
	case c.op <- operation[T]{
		operator: opPub,
		operand: operandInfo[T]{
			pub: msg,
		},
	}:
		return true
	}
}

// TryPub publishes the given message to current caster, so the caster in turn broadcasts the
// message to all subscriber channels without blocking on waiting for channels to be ready for
// receiving. Ok value indicates whether the operation is performed or not. When current caster is
// closed, it stops receiving further operations and the operation won't be performed.
func (c *Caster[T]) TryPub(msg T) (ok bool) {
	okCh := make(chan bool, 1)

	select {
	case <-c.done:
		return false
	case c.op <- operation[T]{
		operator: opTryPub,
		operand: operandInfo[T]{
			pub: msg,
		},
		okCh: okCh,
	}:
		select {
		case ok := <-okCh:
			return ok
		case <-c.done:
			return false
		}
	}
}
