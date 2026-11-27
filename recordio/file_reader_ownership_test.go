package recordio

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thomasjungblut/go-sstables/internal/testutil"
)

func TestReadNextIntoMatchesReadNextAcrossVersionsAndCompression(t *testing.T) {
	paths := []string{
		"test_files/v1_compat/recordio_UncompressedWriterMultiRecord_asc",
		"test_files/v1_compat/recordio_SnappyWriterMultiRecord_asc",
		"test_files/v2_compat/recordio_UncompressedWriterMultiRecord_asc",
		"test_files/v2_compat/recordio_SnappyWriterMultiRecord_asc",
		"test_files/v3_compat/recordio_UncompressedWriterMultiRecord_asc",
		"test_files/v3_compat/recordio_SnappyWriterMultiRecord_asc",
		"test_files/v4_compat/recordio_UncompressedWriterMultiRecord_asc",
		"test_files/v4_compat/recordio_SnappyWriterMultiRecord_asc",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			copyReader, err := newOpenedTestReader(t, path)
			require.NoError(t, err)
			defer closeFileReader(t, copyReader)

			intoReader, err := newOpenedTestReader(t, path)
			require.NoError(t, err)
			defer closeFileReader(t, intoReader)

			var buf []byte
			for {
				expected, expectedErr := copyReader.ReadNext()
				actual, actualErr := intoReader.ReadNextInto(buf)

				if errors.Is(expectedErr, io.EOF) {
					require.ErrorIs(t, actualErr, io.EOF)
					return
				}
				require.NoError(t, expectedErr)
				require.NoError(t, actualErr)
				assert.Equal(t, expected, actual)

				if actual == nil {
					buf = nil
				} else {
					buf = actual[:0]
				}
			}
		})
	}
}

func TestReadNextIntoAliasesCallerBuffer(t *testing.T) {
	for _, compressionType := range []int{CompressionTypeNone, CompressionTypeSnappy, CompressionTypeGZIP, CompressionTypeLzw} {
		t.Run(compressionName(compressionType), func(t *testing.T) {
			record := bytes.Repeat([]byte("reusable"), 512)
			path := filepath.Join(t.TempDir(), "aliases.rio")
			writeTestRecords(t, path, compressionType, [][]byte{record, record})

			reader, err := NewFileReaderWithPath(path)
			require.NoError(t, err)
			require.NoError(t, reader.Open())
			defer closeFileReader(t, reader.(*FileReader))

			buf := make([]byte, 0, len(record))
			for range 2 {
				backing := buf[:cap(buf)]
				actual, err := reader.(*FileReader).ReadNextInto(buf)
				require.NoError(t, err)
				require.Equal(t, record, actual)
				if len(actual) > 0 {
					assert.Same(t, &backing[0], &actual[0])
				}
				buf = actual[:0]
			}
		})
	}
}

func TestReadNextIntoDistinguishesNilAndEmptyRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nil_and_empty.rio")
	writeTestRecords(t, path, CompressionTypeNone, [][]byte{nil, {}})

	reader, err := NewFileReaderWithPath(path)
	require.NoError(t, err)
	require.NoError(t, reader.Open())
	defer closeFileReader(t, reader.(*FileReader))

	nilRecord, err := reader.(*FileReader).ReadNextInto(nil)
	require.NoError(t, err)
	assert.Nil(t, nilRecord)

	emptyRecord, err := reader.(*FileReader).ReadNextInto(nil)
	require.NoError(t, err)
	assert.NotNil(t, emptyRecord)
	assert.Empty(t, emptyRecord)
}

func TestReadNextResultsAreCallerOwned(t *testing.T) {
	firstExpected := testutil.Bytes(128)
	secondExpected := testutil.Bytes(128)
	path := filepath.Join(t.TempDir(), "owned.rio")
	writeTestRecords(t, path, CompressionTypeNone, [][]byte{firstExpected, secondExpected})

	reader, err := NewFileReaderWithPath(path)
	require.NoError(t, err)
	require.NoError(t, reader.Open())

	first, err := reader.ReadNext()
	require.NoError(t, err)
	second, err := reader.ReadNext()
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	assert.Equal(t, firstExpected, first)
	assert.Equal(t, secondExpected, second)

	for i := range first {
		first[i] = 0xff
	}
	firstCopy := bytes.Clone(first)
	assert.Equal(t, secondExpected, second)
	assert.NotEqual(t, firstExpected, first)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.Equal(t, firstCopy, first)
		assert.Equal(t, secondExpected, second)
	}()
	wg.Wait()
}

func TestMMapReadNextAtIntoAndView(t *testing.T) {
	for _, compressionType := range []int{CompressionTypeNone, CompressionTypeSnappy} {
		t.Run(compressionName(compressionType), func(t *testing.T) {
			records := [][]byte{testutil.Bytes(17), testutil.Bytes(4096), nil, {}, testutil.Bytes(128)}
			path := filepath.Join(t.TempDir(), "mmap.rio")
			offsets := writeTestRecords(t, path, compressionType, records)

			reader, err := NewMemoryMappedReaderWithPath(path)
			require.NoError(t, err)
			require.NoError(t, reader.Open())
			defer reader.Close()

			buf := make([]byte, 0, 4096)
			for i, offset := range offsets {
				expected, err := reader.ReadNextAt(offset)
				require.NoError(t, err)

				actual, err := reader.(*MMapReader).ReadNextAtInto(offset, buf)
				require.NoError(t, err)
				assert.Equal(t, expected, actual)

				view, err := reader.(*MMapReader).ReadNextAtView(offset)
				require.NoError(t, err)
				assert.Equal(t, expected, view)

				if actual != nil {
					buf = actual[:0]
				}
				if i > 0 && len(expected) > 0 {
					for j := range expected {
						expected[j] = 0xff
					}
					assert.Equal(t, records[i], view)
				}
			}
		})
	}
}

func TestMMapReadNextAtIntoConcurrentWithDistinctBuffers(t *testing.T) {
	records := make([][]byte, 200)
	for i := range records {
		records[i] = testutil.Bytes(512 + i)
	}
	path := filepath.Join(t.TempDir(), "mmap_concurrent.rio")
	offsets := writeTestRecords(t, path, CompressionTypeNone, records)

	reader, err := NewMemoryMappedReaderWithPath(path)
	require.NoError(t, err)
	require.NoError(t, reader.Open())
	defer reader.Close()

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 0, 4096)
			for i, offset := range offsets {
				actual, err := reader.(*MMapReader).ReadNextAtInto(offset, buf)
				if err != nil {
					errs <- err
					return
				}
				if !bytes.Equal(records[i], actual) {
					errs <- errors.New("record mismatch")
					return
				}
				buf = actual[:0]
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func writeTestRecords(t *testing.T, path string, compressionType int, records [][]byte) []uint64 {
	t.Helper()
	writer, err := NewFileWriter(Path(path), CompressionType(compressionType))
	require.NoError(t, err)
	require.NoError(t, writer.Open())

	offsets := make([]uint64, len(records))
	for i, record := range records {
		offsets[i], err = writer.Write(record)
		require.NoError(t, err)
	}
	require.NoError(t, writer.Close())
	return offsets
}

func compressionName(compressionType int) string {
	switch compressionType {
	case CompressionTypeNone:
		return "none"
	case CompressionTypeSnappy:
		return "snappy"
	case CompressionTypeGZIP:
		return "gzip"
	case CompressionTypeLzw:
		return "lzw"
	default:
		return "unknown"
	}
}
