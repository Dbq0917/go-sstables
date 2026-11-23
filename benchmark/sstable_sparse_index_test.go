package benchmark

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/sstables"
)

const sizeTenGigs = 10 * 1024 * 1024 * 1024

// BenchmarkSSTableSparseIndexRandomRead10G builds a 10GB sstable once and then measures,
// per index granularity, how much heap the loaded index occupies and how fast random reads are.
// Run it with: go test -bench=BenchmarkSSTableSparseIndexRandomRead10G ./benchmark
func BenchmarkSSTableSparseIndexRandomRead10G(b *testing.B) {
	tmpDir := benchDir(b)
	keys := writeSSTableWithSize(b, sizeTenGigs, tmpDir, cmp)
	// dropped only once, the page cache warms up during the run like in any long-running process
	dropPageCache(b, tmpDir)

	loaders := []struct {
		name   string
		loader sstables.IndexLoader
	}{
		{"slice_all_in_memory", &sstables.SliceKeyIndexLoader{ReadBufferSize: 4096}},
		{"sparse_interval_16", &sstables.SparseKeyIndexLoader{Interval: 16}},
		{"sparse_interval_64", &sstables.SparseKeyIndexLoader{Interval: 64}},
		{"sparse_interval_256", &sstables.SparseKeyIndexLoader{Interval: 256}},
		{"disk_nothing_in_memory", &sstables.DiskIndexLoader{}},
	}

	for _, bm := range loaders {
		// the readers are created once outside of the calibrated sub-benchmarks, so the
		// index load time doesn't distort the random read measurements
		runtime.GC()
		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)

		loadStart := time.Now()
		reader, err := sstables.NewSSTableReader(
			sstables.ReadBasePath(tmpDir),
			sstables.ReadWithKeyComparator(cmp),
			sstables.SkipHashCheckOnLoad(),
			sstables.ReadIndexLoader(bm.loader),
		)
		require.NoError(b, err)
		loadMs := float64(time.Since(loadStart).Milliseconds())

		runtime.GC()
		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)
		indexHeapMb := float64(memAfter.HeapAlloc-memBefore.HeapAlloc) / 1024 / 1024

		b.Run(bm.name, func(b *testing.B) {
			randomRead(b, reader, keys)
			// reported after randomRead, because its ResetTimer would clear the metrics
			b.ReportMetric(indexHeapMb, "index_heap_mb")
			b.ReportMetric(float64(reader.MetaData().IndexBytes)/1024/1024, "index_file_mb")
			b.ReportMetric(float64(reader.MetaData().NumRecords), "num_records")
			b.ReportMetric(loadMs, "index_load_ms")
		})

		require.NoError(b, reader.Close())
	}
}
