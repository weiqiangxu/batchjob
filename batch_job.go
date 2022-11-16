package batchjob

import (
	"context"
	"fmt"
	"sync"
)

// Consumer consumer
type Consumer interface {
	// AddTask add task
	AddTask(f func() error)
	// Limit set concurrency number
	Limit(n int)
	// Run start consumer task and get error if catch error
	Run() []error
}

type token struct{}

type ConsumerClient struct {
	ctx          context.Context // root context
	wg           sync.WaitGroup  // wait group make sure all task completed
	speedControl chan token      // consumer concurrency controller
	errMessages  sync.Map        // error message map
	taskIndex    int             // task index
	stop         chan bool       // whether to start execution
}

// NewConsumer Get the task execution object
func NewConsumer(ctx context.Context) Consumer {
	return &ConsumerClient{
		ctx:          ctx,
		wg:           sync.WaitGroup{},
		speedControl: make(chan token, 1),
		stop:         make(chan bool),
		taskIndex:    0,
	}
}

// Run Start Task
func (g *ConsumerClient) Run() []error {
	close(g.stop)
	g.wg.Wait()
	var list []error
	g.errMessages.Range(func(key, value any) bool {
		list = append(list, value.(error))
		return true
	})
	return list
}

// AddTask Add execution task
func (g *ConsumerClient) AddTask(f func() error) {
	g.taskIndex++
	g.wg.Add(1)
	go func(currentIndex int) {
		errTips := fmt.Errorf("task index=%d timeout", currentIndex)
		defer func() {
			g.wg.Done()
		}()
		<-g.stop
		select {
		case <-g.ctx.Done():
			g.errMessages.Store(g.taskIndex, errTips)
			return
		default:
			func() {
				g.speedControl <- token{}
				defer func() {
					<-g.speedControl
				}()
				complete := make(chan bool)
				go func() {
					defer close(complete)
					if err := f(); err != nil {
						select {
						case <-g.ctx.Done():
						default:
							g.errMessages.Store(g.taskIndex, err)
						}
					}
				}()
				select {
				case <-complete:
					return
				case <-g.ctx.Done():
					g.errMessages.Store(g.taskIndex, errTips)
					return
				}
			}()
		}
	}(g.taskIndex)
}

// Limit Set parallel quantity
func (g *ConsumerClient) Limit(n int) {
	if n > 0 {
		g.speedControl = make(chan token, n)
	}
}
