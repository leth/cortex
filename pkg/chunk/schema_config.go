package chunk

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/common/model"
	yaml "gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/weaveworks/common/mtime"
)

const (
	secondsInHour      = int64(time.Hour / time.Second)
	secondsInDay       = int64(24 * time.Hour / time.Second)
	millisecondsInHour = int64(time.Hour / time.Millisecond)
	millisecondsInDay  = int64(24 * time.Hour / time.Millisecond)
)

type PeriodConfig struct {
	From        model.Time          `yaml:"from"`
	Store       string              `yaml:"store"`
	Schema      string              `yaml:"schema"`
	IndexTables PeriodicTableConfig `yaml:"index"`
	ChunkTables PeriodicTableConfig `yaml:"chunks"`
}

// SchemaConfig contains the config for our chunk index schemas
type SchemaConfig struct {
	Configs []PeriodConfig `yaml:"configs"`
}

type LegacySchemaConfig struct {
	// After midnight on this day, we start bucketing indexes by day instead of by
	// hour.  Only the day matters, not the time within the day.
	DailyBucketsFrom      util.DayValue
	Base64ValuesFrom      util.DayValue
	V4SchemaFrom          util.DayValue
	V5SchemaFrom          util.DayValue
	V6SchemaFrom          util.DayValue
	V9SchemaFrom          util.DayValue
	BigtableColumnKeyFrom util.DayValue

	// Config for the index & chunk tables.
	OriginalTableName string
	UsePeriodicTables bool
	IndexTablesFrom   util.DayValue
	IndexTables       PeriodicTableConfig
	ChunkTablesFrom   util.DayValue
	ChunkTables       PeriodicTableConfig
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *LegacySchemaConfig) RegisterFlags(f *flag.FlagSet) {
	f.Var(&cfg.DailyBucketsFrom, "dynamodb.daily-buckets-from", "The date (in the format YYYY-MM-DD) of the first day for which DynamoDB index buckets should be day-sized vs. hour-sized.")
	f.Var(&cfg.Base64ValuesFrom, "dynamodb.base64-buckets-from", "The date (in the format YYYY-MM-DD) after which we will stop querying to non-base64 encoded values.")
	f.Var(&cfg.V4SchemaFrom, "dynamodb.v4-schema-from", "The date (in the format YYYY-MM-DD) after which we enable v4 schema.")
	f.Var(&cfg.V5SchemaFrom, "dynamodb.v5-schema-from", "The date (in the format YYYY-MM-DD) after which we enable v5 schema.")
	f.Var(&cfg.V6SchemaFrom, "dynamodb.v6-schema-from", "The date (in the format YYYY-MM-DD) after which we enable v6 schema.")
	f.Var(&cfg.V9SchemaFrom, "dynamodb.v9-schema-from", "The date (in the format YYYY-MM-DD) after which we enable v9 schema (Series indexing).")
	f.Var(&cfg.BigtableColumnKeyFrom, "bigtable.column-key-from", "The date (in the format YYYY-MM-DD) after which we use bigtable column keys.")

	f.StringVar(&cfg.OriginalTableName, "dynamodb.original-table-name", "cortex", "The name of the DynamoDB table used before versioned schemas were introduced.")
	f.BoolVar(&cfg.UsePeriodicTables, "dynamodb.use-periodic-tables", false, "Should we use periodic tables.")

	f.Var(&cfg.IndexTablesFrom, "dynamodb.periodic-table.from", "Date after which to use periodic tables.")
	cfg.IndexTables.RegisterFlags("dynamodb.periodic-table", "cortex_", f)
	f.Var(&cfg.ChunkTablesFrom, "dynamodb.chunk-table.from", "Date after which to write chunks to DynamoDB.")
	cfg.ChunkTables.RegisterFlags("dynamodb.chunk-table", "cortex_chunks_", f)
}

// translate from command-line parameters into new config data structure
func (schemaCfg *LegacySchemaConfig) TranslateConfig() SchemaConfig {
	config := SchemaConfig{}

	add := func(t string, f model.Time) {
		config.Configs = append(config.Configs, PeriodConfig{From: f, Schema: t})
	}

	add("v1", 0)

	if schemaCfg.DailyBucketsFrom.IsSet() {
		add("v2", schemaCfg.DailyBucketsFrom.Time)
	}
	if schemaCfg.Base64ValuesFrom.IsSet() {
		add("v3", schemaCfg.Base64ValuesFrom.Time)
	}
	if schemaCfg.V4SchemaFrom.IsSet() {
		add("v4", schemaCfg.V4SchemaFrom.Time)
	}
	if schemaCfg.V5SchemaFrom.IsSet() {
		add("v5", schemaCfg.V5SchemaFrom.Time)
	}
	if schemaCfg.V6SchemaFrom.IsSet() {
		add("v6", schemaCfg.V6SchemaFrom.Time)
	}
	if schemaCfg.V9SchemaFrom.IsSet() {
		add("v9", schemaCfg.V9SchemaFrom.Time)
	}

	config.ForEachAfter(schemaCfg.IndexTablesFrom.Time, func(config *PeriodConfig) {
		config.IndexTables = schemaCfg.IndexTables.clean()
	})
	config.ForEachAfter(schemaCfg.ChunkTablesFrom.Time, func(config *PeriodConfig) {
		config.ChunkTables = schemaCfg.ChunkTables.clean()
	})
	config.ForEachAfter(schemaCfg.BigtableColumnKeyFrom.Time, func(config *PeriodConfig) {
		// FIXME
	})

	return config
}

func (tableConfig PeriodicTableConfig) clean() PeriodicTableConfig {
	tableConfig.WriteScale.clean()
	tableConfig.InactiveWriteScale.clean()
	return tableConfig
}

func (c *AutoScalingConfig) clean() {
	if !c.Enabled {
		// Blank the default values from flag since they aren't used
		c.MinCapacity = 0
		c.MaxCapacity = 0
		c.OutCooldown = 0
		c.InCooldown = 0
		c.TargetValue = 0
	}
}

// Given a SchemaConfig, call f() on every entry after t, splitting
// entries if necessary so there is an entry starting at t
func (schemaCfg *SchemaConfig) ForEachAfter(t model.Time, f func(config *PeriodConfig)) {
	for i := 0; i < len(schemaCfg.Configs); i++ {
		if t > schemaCfg.Configs[i].From &&
			(i+1 == len(schemaCfg.Configs) || t < schemaCfg.Configs[i+1].From) {
			// Split the i'th entry by duplicating then overwriting the From time
			schemaCfg.Configs = append(schemaCfg.Configs[:i+1], schemaCfg.Configs[i:]...)
			schemaCfg.Configs[i+1].From = t
		}
		if schemaCfg.Configs[i].From >= t {
			f(&schemaCfg.Configs[i])
		}
	}
}

func (cfg PeriodConfig) createSchema() Schema {
	var s schema
	switch cfg.Schema {
	case "v1":
		s = schema{cfg.hourlyBuckets, originalEntries{}}
	case "v2":
		s = schema{cfg.dailyBuckets, originalEntries{}}
	case "v3":
		s = schema{cfg.dailyBuckets, base64Entries{originalEntries{}}}
	case "v4":
		s = schema{cfg.dailyBuckets, labelNameInHashKeyEntries{}}
	case "v5":
		s = schema{cfg.dailyBuckets, v5Entries{}}
	case "v6":
		s = schema{cfg.dailyBuckets, v6Entries{}}
	case "v9":
		s = schema{cfg.dailyBuckets, v9Entries{}}
	}
	return s
}

func NewCompositeStoreEntry(storeCfg StoreConfig, cfg PeriodConfig, storage StorageClient) (CompositeStoreEntry, error) {
	s := cfg.createSchema()
	f := newStore
	switch cfg.Schema {
	case "v9":
		f = newSeriesStore
	}
	store, err := f(storeCfg, s, storage)
	return CompositeStoreEntry{start: cfg.From, Store: store}, err
}

func (cfg *PeriodConfig) tableForBucket(bucketStart int64) string {
	if cfg.IndexTables.Period == 0 {
		return cfg.IndexTables.Prefix
	}
	// TODO remove reference to time package here
	return cfg.IndexTables.Prefix + strconv.Itoa(int(bucketStart/int64(cfg.IndexTables.Period/time.Second)))
}

func loadConfig(filename string) (*SchemaConfig, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	var config SchemaConfig

	decoder := yaml.NewDecoder(f)
	decoder.SetStrict(true)
	if err := decoder.Decode(&config); err != nil {
		return nil, err
	}

	return &config, nil
}

func (s SchemaConfig) PrintYaml() {
	encoder := yaml.NewEncoder(os.Stdout)
	encoder.Encode(s)
}

// Bucket describes a range of time with a tableName and hashKey
type Bucket struct {
	from      uint32
	through   uint32
	tableName string
	hashKey   string
}

func (cfg *PeriodConfig) hourlyBuckets(from, through model.Time, userID string) []Bucket {
	var (
		fromHour    = from.Unix() / secondsInHour
		throughHour = through.Unix() / secondsInHour
		result      = []Bucket{}
	)

	// If through ends on the hour, don't include the upcoming hour
	if through.Unix()%secondsInHour == 0 {
		throughHour--
	}

	for i := fromHour; i <= throughHour; i++ {
		relativeFrom := util.Max64(0, int64(from)-(i*millisecondsInHour))
		relativeThrough := util.Min64(millisecondsInHour, int64(through)-(i*millisecondsInHour))
		result = append(result, Bucket{
			from:      uint32(relativeFrom),
			through:   uint32(relativeThrough),
			tableName: cfg.tableForBucket(i * secondsInHour),
			hashKey:   fmt.Sprintf("%s:%d", userID, i),
		})
	}
	return result
}

func (cfg *PeriodConfig) dailyBuckets(from, through model.Time, userID string) []Bucket {
	var (
		fromDay    = from.Unix() / secondsInDay
		throughDay = through.Unix() / secondsInDay
		result     = []Bucket{}
	)

	// If through ends on 00:00 of the day, don't include the upcoming day
	if through.Unix()%secondsInDay == 0 {
		throughDay--
	}

	for i := fromDay; i <= throughDay; i++ {
		// The idea here is that the hash key contains the bucket start time (rounded to
		// the nearest day).  The range key can contain the offset from that, to the
		// (start/end) of the chunk. For chunks that span multiple buckets, these
		// offsets will be capped to the bucket boundaries, i.e. start will be
		// positive in the first bucket, then zero in the next etc.
		//
		// The reason for doing all this is to reduce the size of the time stamps we
		// include in the range keys - we use a uint32 - as we then have to base 32
		// encode it.

		relativeFrom := util.Max64(0, int64(from)-(i*millisecondsInDay))
		relativeThrough := util.Min64(millisecondsInDay, int64(through)-(i*millisecondsInDay))
		result = append(result, Bucket{
			from:      uint32(relativeFrom),
			through:   uint32(relativeThrough),
			tableName: cfg.tableForBucket(i * secondsInDay),
			hashKey:   fmt.Sprintf("%s:d%d", userID, i),
		})
	}
	return result
}

// PeriodicTableConfig is configuration for a set of time-sharded tables.
type PeriodicTableConfig struct {
	Prefix string        `yaml:"prefix"`
	Period time.Duration `yaml:"period,omitempty"`
	Tags   Tags          `yaml:"tags,omitempty"`

	ProvisionedWriteThroughput int64 `yaml:"write_throughput,omitempty"`
	ProvisionedReadThroughput  int64 `yaml:"read_throughput,omitempty"`
	InactiveWriteThroughput    int64 `yaml:"inactive_write_throughput,omitempty"`
	InactiveReadThroughput     int64 `yaml:"inactive_read_throughput,omitempty"`

	WriteScale              AutoScalingConfig `yaml:"write_scale,omitempty"`
	InactiveWriteScale      AutoScalingConfig `yaml:"inactive_write_scale,omitempty"`
	InactiveWriteScaleLastN int64             `yaml:"inactive_write_scale_last_n,omitempty"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *PeriodicTableConfig) RegisterFlags(argPrefix, tablePrefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Prefix, argPrefix+".prefix", tablePrefix, "DynamoDB table prefix for period tables.")
	f.DurationVar(&cfg.Period, argPrefix+".period", 7*24*time.Hour, "DynamoDB table period.")
	f.Var(&cfg.Tags, argPrefix+".tag", "Tag (of the form key=value) to be added to all tables under management.")

	f.Int64Var(&cfg.ProvisionedWriteThroughput, argPrefix+".write-throughput", 3000, "DynamoDB table default write throughput.")
	f.Int64Var(&cfg.ProvisionedReadThroughput, argPrefix+".read-throughput", 300, "DynamoDB table default read throughput.")
	f.Int64Var(&cfg.InactiveWriteThroughput, argPrefix+".inactive-write-throughput", 1, "DynamoDB table write throughput for inactive tables.")
	f.Int64Var(&cfg.InactiveReadThroughput, argPrefix+".inactive-read-throughput", 300, "DynamoDB table read throughput for inactive tables.")

	cfg.WriteScale.RegisterFlags(argPrefix+".write-throughput.scale", f)
	cfg.InactiveWriteScale.RegisterFlags(argPrefix+".inactive-write-throughput.scale", f)
	f.Int64Var(&cfg.InactiveWriteScaleLastN, argPrefix+".inactive-write-throughput.scale-last-n", 4, "Number of last inactive tables to enable write autoscale.")
}

// AutoScalingConfig for DynamoDB tables.
type AutoScalingConfig struct {
	Enabled     bool    `yaml:"enabled,omitempty"`
	RoleARN     string  `yaml:"role_arn,omitempty"`
	MinCapacity int64   `yaml:"min_capacity,omitempty"`
	MaxCapacity int64   `yaml:"max_capacity,omitempty"`
	OutCooldown int64   `yaml:"out_cooldown,omitempty"`
	InCooldown  int64   `yaml:"in_cooldown,omitempty"`
	TargetValue float64 `yaml:"target,omitempty"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *AutoScalingConfig) RegisterFlags(argPrefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, argPrefix+".enabled", false, "Should we enable autoscale for the table.")
	f.StringVar(&cfg.RoleARN, argPrefix+".role-arn", "", "AWS AutoScaling role ARN")
	f.Int64Var(&cfg.MinCapacity, argPrefix+".min-capacity", 3000, "DynamoDB minimum provision capacity.")
	f.Int64Var(&cfg.MaxCapacity, argPrefix+".max-capacity", 6000, "DynamoDB maximum provision capacity.")
	f.Int64Var(&cfg.OutCooldown, argPrefix+".out-cooldown", 1800, "DynamoDB minimum seconds between each autoscale up.")
	f.Int64Var(&cfg.InCooldown, argPrefix+".in-cooldown", 1800, "DynamoDB minimum seconds between each autoscale down.")
	f.Float64Var(&cfg.TargetValue, argPrefix+".target-value", 80, "DynamoDB target ratio of consumed capacity to provisioned capacity.")
}

func (cfg *PeriodicTableConfig) periodicTables(from model.Time, beginGrace, endGrace time.Duration) []TableDesc {
	var (
		periodSecs     = int64(cfg.Period / time.Second)
		beginGraceSecs = int64(beginGrace / time.Second)
		endGraceSecs   = int64(endGrace / time.Second)
		firstTable     = from.Unix() / periodSecs
		lastTable      = (mtime.Now().Unix() + beginGraceSecs) / periodSecs
		now            = mtime.Now().Unix()
		result         = []TableDesc{}
	)
	fmt.Printf("periodicTables: %v %v %v %v\n", mtime.Now().Unix(), from, firstTable, lastTable)
	for i := firstTable; i <= lastTable; i++ {
		table := TableDesc{
			// Name construction needs to be consistent with chunk_store.bigBuckets
			Name:             cfg.Prefix + strconv.Itoa(int(i)),
			ProvisionedRead:  cfg.InactiveReadThroughput,
			ProvisionedWrite: cfg.InactiveWriteThroughput,
			Tags:             cfg.Tags,
		}

		// if now is within table [start - grace, end + grace), then we need some write throughput
		if (i*periodSecs)-beginGraceSecs <= now && now < (i*periodSecs)+periodSecs+endGraceSecs {
			table.ProvisionedRead = cfg.ProvisionedReadThroughput
			table.ProvisionedWrite = cfg.ProvisionedWriteThroughput

			if cfg.WriteScale.Enabled {
				table.WriteScale = cfg.WriteScale
			}
		} else if cfg.InactiveWriteScale.Enabled && i >= (lastTable-cfg.InactiveWriteScaleLastN) {
			// Autoscale last N tables
			table.WriteScale = cfg.InactiveWriteScale
		}

		result = append(result, table)
	}
	return result
}

// TableFor calculates the table shard for a given point in time.
func (cfg SchemaConfig) ChunkTableFor(t model.Time) string {
	for i := range cfg.Configs {
		if t > cfg.Configs[i].From && (i+1 == len(cfg.Configs) || t < cfg.Configs[i+1].From) {
			return cfg.Configs[i].ChunkTables.TableFor(t)
		}
	}
	return ""
}

// TableFor calculates the table shard for a given point in time.
func (cfg *PeriodicTableConfig) TableFor(t model.Time) string {
	var (
		periodSecs = int64(cfg.Period / time.Second)
		table      = t.Unix() / periodSecs
	)
	return cfg.Prefix + strconv.Itoa(int(table))
}
