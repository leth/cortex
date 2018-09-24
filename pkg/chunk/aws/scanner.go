package aws

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/weaveworks/cortex/pkg/chunk"
)

// ScanTable reads the whole of a table on multiple goroutines in
// parallel, calling back with batches of results on one of the
// callbacks for each goroutine.
func (a storageClient) ScanTable(ctx context.Context, tableName string, withValue bool, callbacks []func(result chunk.ReadBatch)) error {
	var outerErr error
	projection := hashKey + "," + rangeKey
	if withValue {
		projection += "," + valueKey
	}
	var readerGroup sync.WaitGroup
	readerGroup.Add(len(callbacks))
	for segment, callback := range callbacks {
		go func(segment int, callback func(result chunk.ReadBatch)) {
			p := request.Pagination{
				NewRequest: func() (*request.Request, error) {
					input := &dynamodb.ScanInput{
						TableName:              aws.String(tableName),
						ProjectionExpression:   aws.String(projection),
						Segment:                aws.Int64(int64(segment)),
						TotalSegments:          aws.Int64(int64(len(callbacks))),
						ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
					}
					req, _ := a.DynamoDB.ScanRequest(input)
					req.SetContext(ctx)
					// Override the retryer, since awsSessionFromURL sets it to zero
					req.Retryer = client.DefaultRetryer{NumMaxRetries: a.cfg.backoffConfig.MaxRetries}
					return req, nil
				},
			}

			for p.Next() {
				page := p.Page().(*dynamodb.ScanOutput)
				if cc := page.ConsumedCapacity; cc != nil {
					dynamoConsumedCapacity.WithLabelValues("DynamoDB.ScanTable", *cc.TableName).
						Add(float64(*cc.CapacityUnits))
				}

				callback(&dynamoDBReadResponse{items: page.Items})
			}

			err := p.Err()
			if err != nil {
				outerErr = err
				// TODO: abort all segments
			}
			readerGroup.Done()
		}(segment, callback)
	}
	// Wait until all reader segments have finished
	readerGroup.Wait()
	return outerErr
}
