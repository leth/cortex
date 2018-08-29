package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/go-kit/kit/log/level"
	awscommon "github.com/weaveworks/common/aws"
	"github.com/weaveworks/common/logging"

	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/util"
)

func main() {
	var (
		schemaConfig  chunk.SchemaConfig
		storageConfig storage.Config

		week int
	)

	util.RegisterFlags(&storageConfig, &schemaConfig)
	flag.IntVar(&week, "week", 0, "Week number to scan, e.g. 2497")

	flag.Parse()

	var l logging.Level
	l.Set("debug")
	util.Logger, _ = util.NewPrometheusLogger(l)

	if week == 0 {
		week = int(time.Now().Unix() / int64(7*24*time.Hour/time.Second))
	}

	handler := newHandler()

	for _, arg := range flag.Args() {
		handler.orgs[arg] = struct{}{}
	}

	config, err := awscommon.ConfigFromURL(storageConfig.AWSStorageConfig.DynamoDB.URL)
	checkFatal(err)
	session := session.New(config)
	dynamoDB := dynamodb.New(session)

	fmt.Printf("Week %d\n", week)
	tableName := fmt.Sprintf("%s%d", schemaConfig.ChunkTables.Prefix, week)
	input := &dynamodb.ScanInput{
		TableName:            aws.String(tableName),
		ProjectionExpression: aws.String(hashKey),
		//ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	handler.reset()
	err = dynamoDB.ScanPages(input, handler.handlePage)
	checkFatal(err)
	fmt.Printf("\n")

	for user, count := range handler.counts {
		fmt.Printf("%s, %d\n", user, count)
	}

	delete := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			tableName: handler.requests,
		},
	}
	_ = delete
	//_, err = dynamoDB.BatchWriteItem(delete)
	checkFatal(err)

}

/* TODO: delete v8 schema rows for all instances */

const (
	hashKey  = "h"
	rangeKey = "r"
	valueKey = "c"
)

type handler struct {
	orgs     map[string]struct{}
	requests []*dynamodb.WriteRequest
	counts   map[string]int
}

func newHandler() handler {
	return handler{
		orgs:   map[string]struct{}{},
		counts: map[string]int{},
	}
}

func (h *handler) reset() {
	h.requests = nil
	h.counts = map[string]int{}
}

func (h *handler) handlePage(page *dynamodb.ScanOutput, lastPage bool) bool {
	fmt.Printf(".")
	for _, m := range page.Items {
		v := m[hashKey]
		if v.S != nil {
			key := *v.S
			pos := strings.Index(key, "/")
			if pos < 0 { // unrecognized format
				continue
			}
			org := key[:pos]
			h.counts[org]++
			if _, found := h.orgs[org]; found {
				fmt.Printf("%s\n", key)
				h.requests = append(h.requests, &dynamodb.WriteRequest{
					DeleteRequest: &dynamodb.DeleteRequest{
						Key: map[string]*dynamodb.AttributeValue{
							hashKey: {S: aws.String(key)},
						},
					},
				})
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
