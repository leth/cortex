package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/weaveworks/common/logging"

	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/util"
)

var (
	pagesPerDot int
)

func main() {
	var (
		schemaConfig  chunk.SchemaConfig
		storageConfig storage.Config

		orgsFile string

		week      int
		segments  int
		tableName string
		loglevel  string
		address   string
	)

	util.RegisterFlags(&storageConfig, &schemaConfig)
	flag.StringVar(&address, "address", "localhost:6060", "Address to listen on, for profiling, etc.")
	flag.IntVar(&week, "week", 0, "Week number to scan, e.g. 2497 (0 means current week)")
	flag.IntVar(&segments, "segments", 1, "Number of segments to read in parallel")
	flag.StringVar(&orgsFile, "delete-orgs-file", "", "File containing IDs of orgs to delete")
	flag.StringVar(&loglevel, "log-level", "info", "Debug level: debug, info, warning, error")
	flag.IntVar(&pagesPerDot, "pages-per-dot", 10, "Print a dot per N pages in DynamoDB (0 to disable)")

	flag.Parse()

	var l logging.Level
	l.Set(loglevel)
	util.Logger, _ = util.NewPrometheusLogger(l)

	// HTTP listener for profiling
	go func() {
		checkFatal(http.ListenAndServe(address, nil))
	}()

	orgs := map[int]struct{}{}
	if orgsFile != "" {
		content, err := ioutil.ReadFile(orgsFile)
		checkFatal(err)
		for _, arg := range strings.Fields(string(content)) {
			org, err := strconv.Atoi(arg)
			checkFatal(err)
			orgs[org] = struct{}{}
		}
	}

	if week == 0 {
		week = int(time.Now().Unix() / int64(7*24*time.Hour/time.Second))
	}

	storageClient, err := storage.NewStorageClient(storageConfig, schemaConfig)
	checkFatal(err)

	tableName = fmt.Sprintf("%s%d", schemaConfig.ChunkTables.Prefix, week)
	fmt.Printf("table %s\n", tableName)

	handlers := make([]handler, segments)
	callbacks := make([]func(result chunk.ReadBatch), segments)
	for segment := 0; segment < segments; segment++ {
		handler := newHandler(orgs)
		handler.requests = scanner.delete
		handlers[segment] = handler
		callbacks[segment] = handler.handlePage
	}

	err = storageClient.ScanTable(context.Background(), tableName, callbacks)
	checkFatal(err)
}

/* TODO: delete v8 schema rows for all instances */

type summary struct {
	counts map[int]int
}

func newSummary() summary {
	return summary{
		counts: map[int]int{},
	}
}

func (s *summary) accumulate(b summary) {
	for k, v := range b.counts {
		s.counts[k] += v
	}
}

func (s summary) print() {
	for user, count := range s.counts {
		fmt.Printf("%d, %d\n", user, count)
	}
}

type handler struct {
	pages    int
	orgs     map[int]struct{}
	requests chan chunk.WriteBatch
	summary
}

func newHandler(orgs map[int]struct{}) handler {
	return handler{
		orgs:    orgs,
		summary: newSummary(),
	}
}

func (h *handler) handlePage(page chunk.ReadBatch) {
	h.pages++
	if pagesPerDot > 0 && h.pages%pagesPerDot == 0 {
		fmt.Printf(".")
	}
	for i := 0; i < page.Len(); i++ {
		hashValue := page.HashValue(i)
		org := chunk.OrgFromHash(hashValue)
		if org <= 0 {
			continue
		}
		h.counts[org]++
		if _, found := h.orgs[org]; found {
			h.requests <- storageItem{
				tableName:  x,
				hashValue:  hashValue,
				rangeValue: page.RangeValue(i),
				value:      page.Value(i),
			}
		}
	}
}

func checkFatal(err error) {
	if err != nil {
		level.Error(util.Logger).Log("msg", "fatal error", "err", err)
		os.Exit(1)
	}
}
