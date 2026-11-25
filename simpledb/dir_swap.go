package simpledb

import (
	"fmt"
	"os"
	"strings"

	"github.com/thomasjungblut/go-sstables/internal/fsutil"
)

// SSTableTrashSuffix marks a directory that holds an sstable which was moved aside during a
// compaction swap. It is only deleted after the compacted sstable is durable in its place.
const SSTableTrashSuffix = ".trash"

type dirClass int

const (
	dirClassOther dirClass = iota
	dirClassSSTable
	dirClassCompaction
	dirClassTrash
)

// classifyDir judges what a directory inside the database base path represents.
// Recovery (repairCompactions) and loading (reconstructSSTables) share this classification,
// so both always come to the same conclusion when they see a half-finished directory.
func classifyDir(name string) dirClass {
	switch {
	case strings.HasPrefix(name, SSTableCompactionPathPrefix):
		return dirClassCompaction
	case strings.HasSuffix(name, SSTableTrashSuffix):
		return dirClassTrash
	case strings.HasPrefix(name, SSTablePrefix):
		return dirClassSSTable
	default:
		return dirClassOther
	}
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// swapDirectory replaces the directory at replacementPath with the one at writePath.
// It only uses atomic renames and never copies any data. Every intermediate state is
// recoverable, so an interrupted swap can simply be re-executed:
//
//  1. the old directory is moved aside (replacementPath -> trash), it stays fully intact
//     so a crash can still roll back to the original state
//  2. the new directory is renamed into place (writePath -> replacementPath)
//  3. only now that the new directory is durable in place, the old one is deleted
func swapDirectory(basePath, writePath, replacementPath string) error {
	trashPath := replacementPath + SSTableTrashSuffix

	if !dirExists(writePath) {
		switch {
		case dirExists(replacementPath) && !dirExists(trashPath):
			// the swap already finished, nothing to do
			return nil
		case !dirExists(replacementPath) && dirExists(trashPath):
			// interrupted before the new directory was in place, roll back to the original state
			if err := os.Rename(trashPath, replacementPath); err != nil {
				return err
			}
			return fsutil.SyncDir(basePath)
		case !dirExists(replacementPath):
			return fmt.Errorf("cannot swap %s into %s, neither of them exists", writePath, replacementPath)
		}
		// both the replacement and the trash directory exist: the swap finished,
		// only the trash cleanup below is left
	} else {
		if dirExists(replacementPath) && !dirExists(trashPath) {
			if err := os.Rename(replacementPath, trashPath); err != nil {
				return err
			}
			if err := fsutil.SyncDir(basePath); err != nil {
				return err
			}
		}

		if dirExists(replacementPath) {
			return fmt.Errorf("cannot swap %s into %s, both replacement and trash directory exist",
				writePath, replacementPath)
		}

		if err := os.Rename(writePath, replacementPath); err != nil {
			return err
		}
		if err := fsutil.SyncDir(basePath); err != nil {
			return err
		}
	}

	if err := os.RemoveAll(trashPath); err != nil {
		return err
	}
	return fsutil.SyncDir(basePath)
}
