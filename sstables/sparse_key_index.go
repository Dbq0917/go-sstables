package sstables

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/thomasjungblut/go-sstables/recordio"
	rProto "github.com/thomasjungblut/go-sstables/recordio/proto"
	"github.com/thomasjungblut/go-sstables/skiplist"
	"github.com/thomasjungblut/go-sstables/sstables/proto"
	"golang.org/x/exp/slices"
)

// DefaultSparseIndexInterval is the number of index records that share a single in-memory
// entry when SparseKeyIndexLoader.Interval is not set.
const DefaultSparseIndexInterval = 16

type sparseKey struct {
	key []byte
	// indexFileOffset is the offset of the first index record of the block inside the index file
	indexFileOffset uint64
}

// SparseKeyIndex keeps only every Interval-th entry of the index file in memory. Lookups binary
// search the in-memory entries to find the block a key must be in and then scan that block
// sequentially from the (memory mapped) index file. This trades a small, bounded amount of
// extra work per lookup for a much lower memory footprint compared to SliceKeyIndex.
type SparseKeyIndex struct {
	indexPath string
	reader    rProto.ReadAtI
	entries   []sparseKey
}

func (s *SparseKeyIndex) Open() error {
	reader, err := rProto.NewMMapProtoReaderWithPath(s.indexPath)
	if err != nil {
		return fmt.Errorf("error while creating index reader of sstable in '%s': %w", s.indexPath, err)
	}

	err = reader.Open()
	if err != nil {
		return fmt.Errorf("error while opening index reader of sstable in '%s': %w", s.indexPath, err)
	}

	s.reader = reader
	return nil
}

func (s *SparseKeyIndex) Close() error {
	if s.reader == nil {
		return nil
	}
	return s.reader.Close()
}

// blockStartOffset returns the offset in the index file where the block that may contain
// the target key starts. That's the last in-memory entry with a key <= target, or the
// beginning of the records when the target is smaller than the first indexed key.
func (s *SparseKeyIndex) blockStartOffset(target []byte) uint64 {
	idx, found := slices.BinarySearchFunc(s.entries, target, func(entry sparseKey, t []byte) int {
		return bytes.Compare(entry.key, t)
	})
	if found {
		return s.entries[idx].indexFileOffset
	}
	if idx == 0 {
		return recordio.FileHeaderSizeBytes
	}
	return s.entries[idx-1].indexFileOffset
}

// findEntry scans the index file forward from the start of the target's block and returns
// the index record that matches the target key exactly.
func (s *SparseKeyIndex) findEntry(target []byte) (*proto.IndexEntry, bool, error) {
	offset := s.blockStartOffset(target)
	record := &proto.IndexEntry{}
	for {
		recordOffset, _, err := s.reader.SeekNext(record, offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, false, nil
			}
			return nil, false, err
		}

		cmp := bytes.Compare(record.Key, target)
		if cmp == 0 {
			return record, true, nil
		}
		if cmp > 0 {
			return nil, false, nil
		}
		offset = recordOffset + 1
	}
}

func (s *SparseKeyIndex) Contains(key []byte) (bool, error) {
	_, found, err := s.findEntry(key)
	return found, err
}

func (s *SparseKeyIndex) Get(key []byte) (IndexVal, error) {
	record, found, err := s.findEntry(key)
	if err != nil {
		return IndexVal{}, err
	}
	if !found {
		return IndexVal{}, skiplist.NotFound
	}

	return IndexVal{
		Offset:   record.ValueOffset,
		Checksum: record.Checksum,
	}, nil
}

func (s *SparseKeyIndex) Iterator() (skiplist.IteratorI[[]byte, IndexVal], error) {
	return s.newIterator(recordio.FileHeaderSizeBytes, nil, nil), nil
}

func (s *SparseKeyIndex) IteratorStartingAt(key []byte) (skiplist.IteratorI[[]byte, IndexVal], error) {
	return s.newIterator(s.blockStartOffset(key), key, nil), nil
}

func (s *SparseKeyIndex) IteratorBetween(keyLower []byte, keyHigher []byte) (skiplist.IteratorI[[]byte, IndexVal], error) {
	if bytes.Compare(keyLower, keyHigher) > 0 {
		return nil, errors.New("keyHigher is lower than keyLower")
	}
	return s.newIterator(s.blockStartOffset(keyLower), keyLower, keyHigher), nil
}

func (s *SparseKeyIndex) newIterator(offset uint64, skipBelow []byte, endKeyInclusive []byte) *SparseKeyIndexIterator {
	return &SparseKeyIndexIterator{
		reader:          s.reader,
		entry:           &proto.IndexEntry{},
		nextOffset:      offset,
		skipBelow:       skipBelow,
		endKeyInclusive: endKeyInclusive,
	}
}

type SparseKeyIndexIterator struct {
	reader          rProto.ReadAtI
	entry           *proto.IndexEntry
	nextOffset      uint64
	skipBelow       []byte
	endKeyInclusive []byte
	done            bool
}

func (s *SparseKeyIndexIterator) Next() ([]byte, IndexVal, error) {
	if s.done {
		return nil, IndexVal{}, skiplist.Done
	}

	for {
		recordOffset, _, err := s.reader.SeekNext(s.entry, s.nextOffset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				s.done = true
				return nil, IndexVal{}, skiplist.Done
			}
			return nil, IndexVal{}, err
		}
		s.nextOffset = recordOffset + 1

		if s.skipBelow != nil && bytes.Compare(s.entry.Key, s.skipBelow) < 0 {
			continue
		}
		if s.endKeyInclusive != nil && bytes.Compare(s.entry.Key, s.endKeyInclusive) > 0 {
			s.done = true
			return nil, IndexVal{}, skiplist.Done
		}

		return s.entry.Key, IndexVal{
			Offset:   s.entry.ValueOffset,
			Checksum: s.entry.Checksum,
		}, nil
	}
}

// SparseKeyIndexLoader loads only every Interval-th index record into memory, the remaining
// records are read from the index file on demand. Interval must be a positive number,
// values <= 0 fall back to DefaultSparseIndexInterval. An Interval of 1 keeps every record
// in memory, which behaves like SliceKeyIndexLoader but with additional lookups on disk.
type SparseKeyIndexLoader struct {
	Interval int
}

func (l *SparseKeyIndexLoader) Load(indexPath string, metadata *proto.MetaData) (idx SortedKeyIndex, err error) {
	interval := l.Interval
	if interval <= 0 {
		interval = DefaultSparseIndexInterval
	}

	reader, err := rProto.NewMMapProtoReaderWithPath(indexPath)
	if err != nil {
		return nil, fmt.Errorf("error while creating index reader of sstable in '%s': %w", indexPath, err)
	}

	err = reader.Open()
	if err != nil {
		return nil, fmt.Errorf("error while opening index reader of sstable in '%s': %w", indexPath, err)
	}

	defer func() {
		err = errors.Join(err, reader.Close())
	}()

	capacity := uint64(0)
	if metadata != nil {
		capacity = metadata.NumRecords/uint64(interval) + 1
	}

	entries := make([]sparseKey, 0, capacity)
	record := &proto.IndexEntry{}
	offset := uint64(recordio.FileHeaderSizeBytes)
	i := 0
	for {
		recordOffset, _, err := reader.SeekNext(record, offset)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("error while reading index records of sstable in '%s': %w", indexPath, err)
		}

		if i%interval == 0 {
			key := make([]byte, len(record.Key))
			copy(key, record.Key)
			entries = append(entries, sparseKey{key: key, indexFileOffset: recordOffset})
		}

		i++
		offset = recordOffset + 1
	}

	return &SparseKeyIndex{indexPath: indexPath, entries: entries}, nil
}
