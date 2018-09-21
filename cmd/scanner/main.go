package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/go-kit/kit/log/level"
	awscommon "github.com/weaveworks/common/aws"
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

		scannerConfig chunk.ScannerConfig
		loglevel      string
		address       string
	)

	util.RegisterFlags(&storageConfig, &schemaConfig, &scannerConfig)
	flag.StringVar(&address, "address", "localhost:6060", "Address to listen on, for profiling, etc.")
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

	if scannerConfig.week == 0 {
		scannerConfig.week = int(time.Now().Unix() / int64(7*24*time.Hour/time.Second))
	}

	scanner, err := chunk.NewScanner(scannerConfig, storageClient, schemaConfig)
	checkFatal(err)

	handlers := make([]handler, scannerConfig.Segments)
	callbacks := make([]func(result chunk.ReadBatch), scannerConfig.Segments)
	for segment := 0; segment < scannerConfig.Segments; segment++ {
		handler := newHandler(orgs)
		handler.requests = scanner.delete
		handlers[i] = handler
		callbacks[i] = handler.handlePage
	}

	err = scanner.Scan(context.Background(), orgs, callbacks)
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
	requests chan storageItem
	summary
}

func newHandler(orgs map[int]struct{}) handler {
	return handler{
		orgs:    orgs,
		summary: newSummary(),
	}
}

func (h *handler) handlePage(page ReadBatch) bool {
	h.pages++
	if pagesPerDot > 0 && h.pages%pagesPerDot == 0 {
		fmt.Printf(".")
	}
	for i := 0; i < page.Len(); i++ {
		hashValue := page.HashValue(i)
		org := orgFromHash(hashValue)
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
	return true
}

func checkFatal(err error) {
	if err != nil {
		level.Error(util.Logger).Log("msg", "fatal error", "err", err)
		os.Exit(1)
	}
}
