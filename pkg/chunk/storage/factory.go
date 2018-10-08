package storage

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/aws"
	"github.com/cortexproject/cortex/pkg/chunk/cache"
	"github.com/cortexproject/cortex/pkg/chunk/cassandra"
	"github.com/cortexproject/cortex/pkg/chunk/gcp"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
)

// Config chooses which storage client to use.
type Config struct {
	AWSStorageConfig       aws.StorageConfig
	GCPStorageConfig       gcp.Config
	CassandraStorageConfig cassandra.Config

	IndexCacheSize     int
	IndexCacheValidity time.Duration
	memcacheClient     cache.MemcachedClientConfig
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.AWSStorageConfig.RegisterFlags(f)
	cfg.GCPStorageConfig.RegisterFlags(f)
	cfg.CassandraStorageConfig.RegisterFlags(f)

	f.IntVar(&cfg.IndexCacheSize, "store.index-cache-size", 0, "Size of in-memory index cache, 0 to disable.")
	f.DurationVar(&cfg.IndexCacheValidity, "store.index-cache-validity", 5*time.Minute, "Period for which entries in the index cache are valid. Should be no higher than -ingester.max-chunk-idle.")
	cfg.memcacheClient.RegisterFlagsWithPrefix("index", f)
}

// NewStore makes the storage clients based on the configuration.
func NewStore(cfg Config, storeCfg chunk.StoreConfig, schemaCfg chunk.SchemaConfig) (chunk.Store, error) {
	var caches []cache.Cache
	if cfg.IndexCacheSize > 0 {
		fifocache := cache.Instrument("fifo-index", cache.NewFifoCache("index", cfg.IndexCacheSize, cfg.IndexCacheValidity))
		caches = append(caches, fifocache)
	}

	if cfg.memcacheClient.Host != "" {
		client := cache.NewMemcachedClient(cfg.memcacheClient)
		memcache := cache.Instrument("memcache-index", cache.NewMemcached(cache.MemcachedConfig{
			Expiration: cfg.IndexCacheValidity,
		}, client))
		caches = append(caches, cache.NewBackground("memcache-index", cache.BackgroundConfig{
			WriteBackGoroutines: 10,
			WriteBackBuffer:     100,
		}, memcache))
	}

	var tieredCache cache.Cache
	if len(caches) > 0 {
		tieredCache = cache.Instrument("tiered-index", cache.NewTiered(caches))
	}

	stores := []chunk.CompositeStoreEntry{}

	for _, s := range schemaCfg.Configs {
		storage, err := nameToStorage(s.Store, cfg, schemaCfg)
		if err != nil {
			return nil, errors.Wrap(err, "error creating storage client")
		}

		if tieredCache != nil {
			storage = newCachingStorageClient(storage, tieredCache, cfg.IndexCacheValidity)
		}

		c, err := chunk.NewCompositeStoreEntry(storeCfg, s, storage)
		if err != nil {
			return nil, err
		}
		stores = append(stores, c)
	}

	return chunk.NewStore(stores)
}

func nameToStorage(name string, cfg Config, schemaCfg chunk.SchemaConfig) (chunk.StorageClient, error) {
	switch name {
	case "inmemory":
		return chunk.NewMockStorage(), nil
	case "aws":
		return aws.NewS3StorageClient(cfg.AWSStorageConfig, schemaCfg)
	case "aws-dynamo":
		if cfg.AWSStorageConfig.DynamoDB.URL == nil {
			return nil, fmt.Errorf("Must set -dynamodb.url in aws mode")
		}
		path := strings.TrimPrefix(cfg.AWSStorageConfig.DynamoDB.URL.Path, "/")
		if len(path) > 0 {
			level.Warn(util.Logger).Log("msg", "ignoring DynamoDB URL path", "path", path)
		}
		return aws.NewStorageClient(cfg.AWSStorageConfig.DynamoDBConfig, schemaCfg)
	case "gcp":
		return gcp.NewStorageClientV1(context.Background(), cfg.GCPStorageConfig, schemaCfg)
	case "gcp-columnkey":
		return gcp.NewStorageClientColumnKey(context.Background(), cfg.GCPStorageConfig, schemaCfg)
	case "cassandra":
		return cassandra.NewStorageClient(cfg.CassandraStorageConfig, schemaCfg)
	}
	return nil, fmt.Errorf("Unrecognized storage client %v, choose one of: aws, gcp, cassandra, inmemory", name)
}

// NewTableClient makes a new table client based on the configuration.
func NewTableClient(name string, cfg Config) (chunk.TableClient, error) {
	switch name {
	case "inmemory":
		return chunk.NewMockStorage(), nil
	case "aws":
		path := strings.TrimPrefix(cfg.AWSStorageConfig.DynamoDB.URL.Path, "/")
		if len(path) > 0 {
			level.Warn(util.Logger).Log("msg", "ignoring DynamoDB URL path", "path", path)
		}
		return aws.NewDynamoDBTableClient(cfg.AWSStorageConfig.DynamoDBConfig)
	case "gcp":
		return gcp.NewTableClient(context.Background(), cfg.GCPStorageConfig)
	case "cassandra":
		return cassandra.NewTableClient(context.Background(), cfg.CassandraStorageConfig)
	default:
		return nil, fmt.Errorf("Unrecognized storage client %v, choose one of: aws, gcp, inmemory", name)
	}
}
