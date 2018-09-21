package chunk

import (
	"flag"
	"fmt"
)

type ScannerConfig struct {
	Week      int
	Segments  int
	tableName string
}

type Scanner struct {
	ScannerConfig

	storage StorageClient
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *ScannerConfig) RegisterFlags(f *flag.FlagSet) {
	flag.IntVar(&cfg.Week, "week", 0, "Week number to scan, e.g. 2497 (0 means current week)")
	flag.IntVar(&cfg.Segments, "segments", 1, "Number of segments to read in parallel")
}

func NewScanner(cfg ScannerConfig, schemaConfig SchemaConfig, storage StorageClient) (*Scanner, error) {
	scanner := &Scanner{
		storage: storage,
	}

	scanner.tableName = fmt.Sprintf("%s%d", schemaConfig.ChunkTables.Prefix, scanner.Week)
	fmt.Printf("table %s\n", scanner.tableName)

	return scanner, nil
}
