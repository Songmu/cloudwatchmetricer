package cloudwatchmetricer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
)

type Metricer interface {
	Metrics(context.Context) ([]*Metric, error)
}

type Metric struct {
	Name       string
	Value      float64
	Unit       string
	Timestamp  time.Time
	Dimensions map[string]string
}

type Func func(context.Context) ([]*Metric, error)

func (fn Func) Metrics(ctx context.Context) ([]*Metric, error) {
	return fn(ctx)
}

var _ Metricer = Func(func(context.Context) ([]*Metric, error) { return nil, nil })

type Broker struct {
	ErrorHandler func(error)
	svc          cloudwatchiface.CloudWatchAPI

	registerdTasks chan *task
	taskCh         chan *task

	done bool
}

func New(svc cloudwatchiface.CloudWatchAPI) *Broker {
	return &Broker{
		registerdTasks: make(chan *task, 100),
		taskCh:         make(chan *task, 100),
		svc:            svc,
	}
}

func (b *Broker) Run(ctx context.Context) error {
	defer close(b.registerdTasks)

	wg := &sync.WaitGroup{}
	go func() {
		for ta := range b.registerdTasks {
			wg.Add(1)
			go func(ta *task) {
				defer wg.Done()
				ta.start(ctx, b.taskCh)
			}(ta)
		}
	}()

	taskWg := &sync.WaitGroup{}
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case ta := <-b.taskCh:
			taskWg.Add(1)
			go func(ta *task) {
				defer taskWg.Done()
				b.putMetics(ctx, ta)
			}(ta)
		}
	}
	b.done = true
	wg.Wait()

	// draining
	close(b.taskCh)
	for ta := range b.taskCh {
		taskWg.Add(1)
		go func(ta *task) {
			defer taskWg.Done()
			b.putMetics(ctx, ta)
		}(ta)
	}
	taskWg.Wait()
	return nil
}

func (b *Broker) putMetics(ctx context.Context, ta *task) {
	params, err := ta.getMetricData(ctx)
	if err != nil {
		// XXX needs to define error types?
		b.handleError(
			fmt.Errorf("failed to getMetricData. task: %s, err: %w", ta.name, err))
		return
	}
	if _, err := b.svc.PutMetricDataWithContext(ctx, params); err != nil {
		b.handleError(
			fmt.Errorf("failed to PutMetricData. task: %s, err: %w", ta.name, err))
	}
}

func (b *Broker) handleError(err error) {
	if b.ErrorHandler != nil {
		b.ErrorHandler(err)
	} else {
		log.Println(err)
	}
}

func (b *Broker) Register(name, namespace string, interval time.Duration, me Metricer) error {
	if b.done {
		return fmt.Errorf("cannot register tasks because the broker is done")
	}
	ta := &task{
		name:      name,
		namespace: namespace,
		metricer:  me,
		interval:  interval,
	}
	b.registerdTasks <- ta
	return nil
}
