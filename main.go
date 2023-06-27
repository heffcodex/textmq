package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type dequeTask struct {
	ctx   context.Context
	queue string
	value chan<- string
}

const (
	dequeBackoff    = 100 * time.Millisecond
	shutdownTimeout = 5 * time.Second
)

var (
	mq sync.Map
	rq Queue[dequeTask]
)

type element[T any] struct {
	next  *element[T]
	value T
}

type Queue[T any] struct {
	mu   sync.Mutex
	done chan struct{}
	pop  chan T
	head *element[T]
	tail *element[T]
}

func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{}
}

func (q *Queue[T]) Start() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.done != nil {
		panic("queue already started")
	}

	q.done = make(chan struct{})
	q.pop = make(chan T)

	go q.popReceiver()
}

func (q *Queue[T]) Stop() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.done != nil {
		return
	}

	close(q.done)
	close(q.pop)

	q.done = nil
	q.head = nil
	q.tail = nil
}

func (q *Queue[T]) Push(v T) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.done == nil {
		panic("queue closed")
	}

	el := &element[T]{value: v}

	if q.tail == nil {
		q.tail = el
	} else {
		q.tail.next = el
		q.tail = q.tail.next
	}

	if q.head == nil {
		q.head = q.tail
	}
}

func (q *Queue[T]) Receiver() <-chan T {
	return q.pop
}

func (q *Queue[T]) popReceiver() {
	t := time.NewTimer(0)
	defer t.Stop()

	var value T
	var ok bool

	for {
		select {
		case <-q.done:
			return
		case <-t.C:
			value, ok = q.tryPop()
		}

		if !ok {
			t.Reset(dequeBackoff)
			continue
		}

		select {
		case <-q.done:
			return
		case q.pop <- value:
			t.Reset(0)
		}
	}
}

func (q *Queue[T]) tryPop() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	el := q.head
	if el == nil {
		return *new(T), false
	}

	q.head = el.next

	if q.head == nil {
		q.tail = nil
	}

	return el.value, true
}

func main() {
	port := flag.Uint64("port", 8080, "port to listen on")
	flag.Parse()

	rq.Start()
	go dequeWorker()

	defer func() {
		rq.Stop()
		mq.Range(func(_, value any) bool {
			value.(*Queue[string]).Stop()
			return true
		})
	}()

	http.HandleFunc("/", handler)

	srv := http.Server{
		Addr: ":" + strconv.FormatUint(*port, 10),
	}

	go func() {
		err := srv.ListenAndServe()
		if err != http.ErrServerClosed {
			log.Fatalln(fmt.Errorf("server listen: %w", err))
		}
	}()

	log.Println("listening on", srv.Addr)

	waitInterrupt()

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	err := srv.Shutdown(ctx)
	if err != nil {
		log.Fatalln(fmt.Errorf("server shutdown: %w", err))
	}
}

func waitInterrupt() {
	sig := make(chan os.Signal, 1)

	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	signal.Stop(sig)
}

func handler(w http.ResponseWriter, r *http.Request) {
	queue := strings.Trim(r.URL.Path, "/")
	if queue == "" || strings.Contains(queue, "/") {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		timeout, err := parseTimeout(r.URL.Query())
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		ctx := r.Context()

		if timeout > 0 {
			var cancel context.CancelFunc

			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		handleGET(ctx, w, queue)
	case http.MethodPut:
		value := r.URL.Query().Get("v")
		if value == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		getQueue(queue).Push(value)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func parseTimeout(query url.Values) (time.Duration, error) {
	timeout := query.Get("timeout")
	if timeout == "" {
		return 0, nil
	}

	t, err := strconv.Atoi(timeout)
	if err != nil || t < 0 {
		return 0, fmt.Errorf("bad timeout: %w", err)
	}

	return time.Duration(t) * time.Second, nil
}

func handleGET(ctx context.Context, w http.ResponseWriter, queue string) {
	vChan := make(chan string)
	defer close(vChan)

	rq.Push(dequeTask{
		ctx:   ctx,
		queue: queue,
		value: vChan,
	})

	value := <-vChan
	if value == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	_, err := w.Write([]byte(value))
	if err != nil {
		log.Println(fmt.Errorf("response write: %w", err))
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func getQueue(queue string) *Queue[string] {
	q, loaded := mq.Load(queue)
	if !loaded {
		qNew := NewQueue[string]()

		q, loaded = mq.LoadOrStore(queue, qNew)
		if !loaded {
			qNew.Start()
		}
	}

	return q.(*Queue[string])
}

func dequeWorker() {
	for task := range rq.Receiver() {
		qRecv := getQueue(task.queue).Receiver()

		select {
		case v := <-qRecv:
			task.value <- v
		case <-task.ctx.Done():
			task.value <- ""
		}
	}
}
