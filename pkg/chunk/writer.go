package chunk

import (
	"context"
	"flag"
	"sync"

	"github.com/go-kit/kit/log/level"

	"github.com/weaveworks/cortex/pkg/util"
)

type WriterConfig struct {
	writers        int
	writeBatchSize int
}

type storageItem struct {
	tableName  string
	hashValue  string
	rangeValue []byte
	value      []byte
}

type Writer struct {
	WriterConfig

	storage StorageClient

	group   sync.WaitGroup
	pending sync.WaitGroup

	// Clients send items on this chan to be written
	Write chan *storageItem
	// internally we send items on this chan to be retried
	retry chan *storageItem
	// Writers read batches of items from this chan
	batched chan WriteBatch
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *WriterConfig) RegisterFlags(f *flag.FlagSet) {
	flag.IntVar(&cfg.writers, "writers", 1, "Number of writers to run in parallel")
	flag.IntVar(&cfg.writeBatchSize, "write-batch-size", 25, "Number of write requests to batch up")
}

func NewWriter(cfg ScannerConfig, storage StorageClient) *Writer {
	writer := &Writer{
		storage: storage,
		// Unbuffered chan so we can tell when batcher has received all items
		Write:   make(chan *storageItem),
		retry:   make(chan *storageItem, 100),
		batched: make(chan WriteBatch),
	}
	return writer
}

func (writer *Writer) Run() {
	writer.group.Add(1 + writer.writers)
	go func() {
		writer.batcher()
		writer.group.Done()
	}()
	for i := 0; i < writer.writers; i++ {
		go func() {
			writer.writeLoop()
			writer.group.Done()
		}()
	}
}

func (writer *Writer) Stop() {
	// Ensure that batcher has received all items so it won't call Add() any more
	writer.Write <- nil
	// Wait for pending items to be sent to store
	writer.pending.Wait()
	// Close chans to signal writer(s) and batcher to terminate
	close(writer.batched)
	close(writer.retry)
	writer.group.Wait()
}

func (sc *Writer) writeLoop() {
	for {
		batch, ok := <-sc.batched
		if !ok {
			return
		}
		level.Debug(util.Logger).Log("msg", "about to write", "num_requests", batch.Len())
		retry, err := sc.storage.BatchWriteNoRetry(context.TODO(), batch)
		if err != nil {
			level.Error(util.Logger).Log("msg", "unable to write; dropping data", "err", err)
			sc.pending.Add(-batch.Len())
			continue
		}
		// Send unprocessed items back into the batcher
		for _, item := range retry {
			sc.retry <- &item
		}
		sc.pending.Add(-(batch.Len() - len(retry)))
	}
}

// Receive individual requests, and batch them up into groups to send to the store
func (sc *Writer) batcher() {
	finished := false
	var requests []*storageItem
	for {
		// We will allow in new data if the queue isn't too long
		var in chan *storageItem
		if len(requests) < 1000 {
			in = sc.Write
		}
		// We will send out a batch if the queue is big enough, or if we're finishing
		var out chan WriteBatch
		outlen := len(requests)
		if len(requests) >= sc.writeBatchSize {
			out = sc.batched
			outlen = sc.writeBatchSize
		} else if finished && len(requests) > 0 {
			out = sc.batched
		}
		outBatch := sc.storage.NewWriteBatch()
		for _, item := range requests[:outlen] {
			outBatch.Add(item.tableName, item.hashValue, item.rangeValue, item.value)
		}
		var item *storageItem
		var ok bool
		select {
		case item = <-in:
			if item == nil { // Nil used as interlock to know we received all previous values
				finished = true
			} else {
				sc.pending.Add(1)
			}
		case item, ok = <-sc.retry:
			if !ok {
				return
			}
		case out <- outBatch:
			requests = requests[outlen:]
		}
		if item != nil {
			requests = append(requests, item)
		}
	}
}
