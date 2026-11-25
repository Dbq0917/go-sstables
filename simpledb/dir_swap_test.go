package simpledb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeMarkerDir(t *testing.T, path string, marker string) {
	require.NoError(t, os.MkdirAll(path, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(path, "marker"), []byte(marker), 0600))
}

func readMarker(t *testing.T, path string) string {
	content, err := os.ReadFile(filepath.Join(path, "marker"))
	require.NoError(t, err)
	return string(content)
}

func TestSwapDirectoryHappyPath(t *testing.T) {
	dir := t.TempDir()
	writePath := filepath.Join(dir, "sstable_compaction-1")
	replacementPath := filepath.Join(dir, "sstable_000000000000001")
	writeMarkerDir(t, writePath, "new")
	writeMarkerDir(t, replacementPath, "old")

	require.NoError(t, swapDirectory(dir, writePath, replacementPath))

	assert.False(t, dirExists(writePath))
	assert.False(t, dirExists(replacementPath+SSTableTrashSuffix))
	assert.Equal(t, "new", readMarker(t, replacementPath))
}

func TestSwapDirectoryInterruptedAfterOldMovedAside(t *testing.T) {
	dir := t.TempDir()
	writePath := filepath.Join(dir, "sstable_compaction-1")
	replacementPath := filepath.Join(dir, "sstable_000000000000001")
	// crashed after the old directory was moved aside, but before the new one was in place
	writeMarkerDir(t, writePath, "new")
	writeMarkerDir(t, replacementPath+SSTableTrashSuffix, "old")

	require.NoError(t, swapDirectory(dir, writePath, replacementPath))

	assert.False(t, dirExists(writePath))
	assert.False(t, dirExists(replacementPath+SSTableTrashSuffix))
	assert.Equal(t, "new", readMarker(t, replacementPath))
}

func TestSwapDirectoryInterruptedAfterNewInPlace(t *testing.T) {
	dir := t.TempDir()
	writePath := filepath.Join(dir, "sstable_compaction-1")
	replacementPath := filepath.Join(dir, "sstable_000000000000001")
	// crashed after the new directory was in place, but before the trash was deleted
	writeMarkerDir(t, replacementPath, "new")
	writeMarkerDir(t, replacementPath+SSTableTrashSuffix, "old")

	require.NoError(t, swapDirectory(dir, writePath, replacementPath))

	assert.False(t, dirExists(replacementPath+SSTableTrashSuffix))
	assert.Equal(t, "new", readMarker(t, replacementPath))
}

func TestSwapDirectoryRollsBackWhenNewNeverInPlace(t *testing.T) {
	dir := t.TempDir()
	writePath := filepath.Join(dir, "sstable_compaction-1")
	replacementPath := filepath.Join(dir, "sstable_000000000000001")
	// the new directory is gone (e.g. its compaction was discarded), only the old
	// directory moved aside is left: the swap must roll back to the original state
	writeMarkerDir(t, replacementPath+SSTableTrashSuffix, "old")

	require.NoError(t, swapDirectory(dir, writePath, replacementPath))

	assert.False(t, dirExists(replacementPath+SSTableTrashSuffix))
	assert.Equal(t, "old", readMarker(t, replacementPath))
}

func TestSwapDirectoryAlreadyFinished(t *testing.T) {
	dir := t.TempDir()
	writePath := filepath.Join(dir, "sstable_compaction-1")
	replacementPath := filepath.Join(dir, "sstable_000000000000001")
	writeMarkerDir(t, replacementPath, "new")

	require.NoError(t, swapDirectory(dir, writePath, replacementPath))

	assert.False(t, dirExists(replacementPath+SSTableTrashSuffix))
	assert.Equal(t, "new", readMarker(t, replacementPath))
}

func TestSwapDirectoryNothingExists(t *testing.T) {
	dir := t.TempDir()
	err := swapDirectory(dir, filepath.Join(dir, "missing"), filepath.Join(dir, "also-missing"))
	assert.NotNil(t, err)
}

func TestClassifyDir(t *testing.T) {
	assert.Equal(t, dirClassSSTable, classifyDir("sstable_000000000000001"))
	assert.Equal(t, dirClassCompaction, classifyDir("sstable_compaction-123"))
	assert.Equal(t, dirClassTrash, classifyDir("sstable_000000000000001"+SSTableTrashSuffix))
	assert.Equal(t, dirClassOther, classifyDir("wal"))
	assert.Equal(t, dirClassOther, classifyDir("something-else"))
}
