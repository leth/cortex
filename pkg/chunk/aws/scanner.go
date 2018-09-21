package aws

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/weaveworks/cortex/pkg/chunk"
)

func throttled(err error) bool {
	awsErr, ok := err.(awserr.Error)
	return ok && (awsErr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException)
}

func (a storageClient) ScanTable(ctx context.Context, tableName string, callbacks []func(result chunk.ReadBatch)) error {
	var outerErr error
	var readerGroup sync.WaitGroup
	readerGroup.Add(len(callbacks))
	for segment, callback := range callbacks {
		go func(segment int, callback func(result chunk.ReadBatch)) {
			err := a.segmentScan(segment, len(callbacks), tableName, callback)
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

func (a storageClient) segmentScan(segment, totalSegments int, tableName string, callback func(result chunk.ReadBatch)) error {
	input := &dynamodb.ScanInput{
		TableName:            aws.String(tableName),
		ProjectionExpression: aws.String(hashKey + "," + rangeKey),
		Segment:              aws.Int64(int64(segment)),
		TotalSegments:        aws.Int64(int64(totalSegments)),
		//ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}

	err := a.DynamoDB.ScanPages(input, func(page *dynamodb.ScanOutput, lastPage bool) bool {
		callback(&dynamoDBReadResponse{items: page.Items})
		return true
	})
	if err != nil {
		return err
	}
	return nil
}

func (a storageClient) BatchWriteNoRetry(ctx context.Context, batch chunk.WriteBatch) (retry chunk.WriteBatch, err error) {
	dynamoBatch := batch.(dynamoDBWriteBatch)
	ret, err := a.DynamoDB.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: dynamoBatch,
	})
	if err != nil {
		if throttled(err) {
			// Send the whole request back
			return dynamoBatch, nil
		} else {
			return nil, err
		}
	}
	// Send unprocessed items back
	return dynamoDBWriteBatch(ret.UnprocessedItems), nil
}
