package sstables

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
)

func writeSparseIndexTestTable(t *testing.T, numRecords int) *SSTableStreamWriter {
	writer, err := newTestSSTableStreamWriter()
	require.NoError(t, err)
	t.Cleanup(func() { cleanWriterDir(t, writer) })

	require.NoError(t, writer.Open())
	for i := 0; i < numRecords; i++ {
		k, v := getKeyValueAsBytes(i)
		require.NoError(t, writer.WriteNext(k, v))
	}
	require.NoError(t, writer.Close())
	return writer
}

func loadSparseIndex(t *testing.T, indexPath string, interval int) SortedKeyIndex {
	idx, err := (&SparseKeyIndexLoader{Interval: interval}).Load(indexPath, &proto.MetaData{})
	require.NoError(t, err)
	require.NoError(t, idx.Open())
	t.Cleanup(func() { require.NoError(t, idx.Close()) })
	return idx
}

func TestSparseIndexGetContains(t *testing.T) {
	numRecords := 1000
	writer := writeSparseIndexTestTable(t, numRecords)

	for _, interval := range []int{1, 2, 3, 7, 16, 64, 1000, 0} {
		t.Run(fmt.Sprintf("interval_%d", interval), func(t *testing.T) {
			idx := loadSparseIndex(t, writer.indexFilePath, interval)

			for i := 0; i < numRecords; i++ {
				key, _ := getKeyValueAsBytes(i)
				v, err := idx.Get(key)
				require.NoError(t, err)
				require.NotNil(t, v)

				contains, err := idx.Contains(key)
				require.NoError(t, err)
				assert.True(t, contains)
			}

			// holes between existing keys and out of range keys must not be found
			for _, missing := range [][]byte{{}, {1}, {1, 2, 3}, intToByteSlice(numRecords), intToByteSlice(numRecords + 1)} {
				v, err := idx.Get(missing)
				require.Equal(t, skiplist.NotFound, err)
				require.Equal(t, IndexVal{}, v)

				contains, err := idx.Contains(missing)
				require.NoError(t, err)
				assert.False(t, contains)
			}
		})
	}
}

func TestSparseIndexIterators(t *testing.T) {
	numRecords := 100
	writer := writeSparseIndexTestTable(t, numRecords)

	all := make([]int, 0, numRecords)
	for i := 0; i < numRecords; i++ {
		all = append(all, i)
	}

	for _, interval := range []int{1, 2, 3, 7, 16, 1000, 0} {
		t.Run(fmt.Sprintf("interval_%d", interval), func(t *testing.T) {
			idx := loadSparseIndex(t, writer.indexFilePath, interval)

			it, err := idx.Iterator()
			require.NoError(t, err)
			assertIndexIteratorMatchesSlice(t, it, all)

			// staggered starts, both on and between the in-memory entries
			for start := 0; start < numRecords; start++ {
				it, err := idx.IteratorStartingAt(intToByteSlice(start))
				require.NoError(t, err)
				assertIndexIteratorMatchesSlice(t, it, all[start:])
			}

			it, err = idx.IteratorStartingAt(intToByteSlice(numRecords))
			require.NoError(t, err)
			k, v, err := it.Next()
			require.Equal(t, skiplist.Done, err)
			require.Nil(t, k)
			require.Equal(t, IndexVal{}, v)

			// ranges, both on and between the in-memory entries
			for lower := 0; lower < numRecords; lower += 9 {
				for higher := lower; higher < numRecords; higher += 11 {
					it, err := idx.IteratorBetween(intToByteSlice(lower), intToByteSlice(higher))
					require.NoError(t, err)
					assertIndexIteratorMatchesSlice(t, it, all[lower:higher+1])
				}
			}

			_, err = idx.IteratorBetween(intToByteSlice(1), intToByteSlice(0))
			require.Error(t, err)
		})
	}
}

func TestSparseIndexReaderEndToEnd(t *testing.T) {
	numRecords := 1000
	writer := writeSparseIndexTestTable(t, numRecords)

	reader, err := NewSSTableReader(
		ReadBasePath(writer.opts.basePath),
		ReadWithKeyComparator(skiplist.BytesComparator{}),
		SkipHashCheckOnLoad(),
		ReadIndexLoader(&SparseKeyIndexLoader{Interval: 7}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	for i := 0; i < numRecords; i++ {
		key, value := getKeyValueAsBytes(i)
		actual, err := reader.Get(key)
		require.NoError(t, err)
		assert.Equal(t, value, actual)
	}

	_, err = reader.Get(intToByteSlice(numRecords))
	require.Equal(t, NotFound, err)

	all := make([]int, 0, numRecords)
	for i := 0; i < numRecords; i++ {
		all = append(all, i)
	}
	assertContentMatchesSlice(t, reader, all)
}
