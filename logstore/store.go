// Heavily modified version of the following two github repositories:
// github.com/tidwall/raft-wal
// github.com/tidwall/wal
//
// LogStore and Log structs merged.
// This coupling allows us to rewrite caching to be specific to our use case
//
// truncateFront and truncateBack are modified to support a much faster delete
// truncateFront only removes logs from disk at the segment grain size
// truncateBack removes all requested logs from disk immediately
// Overall, this makes DeleteRange much faster
//
// Cache rewritten
// Cache lookup is now O(1)
// Cache now caches *raft.Log entries in a slice, separate from segments
// Cache only invalidates entries that are deleted by DeleteRange
// This means that all files on disk are also in memory
//
// JSON writing removed.
//
// We write to file using MMAP syscalls now.
// This improves write and sync times dramatically
// For other operations such as file resizing, we take a more expensive route
// and use the file functions, then cycle the MMAP to match the new file size
// NOTE: the file size MUST remain a multiple of the OS PAGESIZE otherwise,
// you are gonna have a bad time

package logstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/hashicorp/raft"
)

var (
	// ErrCorrupt is returns when the log is corrupt.
	ErrCorrupt = errors.New("log corrupt")

	// ErrClosed is returned when an operation
	// cannot be completed because the log is closed.
	ErrClosed = errors.New("log closed")

	// ErrNotFound is returned when an entry is not found.
	ErrNotFound = errors.New("not found")

	// ErrOutOfOrder is returned from Write() when the index is not equal to
	// LastIndex()+1. It's required that log monotonically grows by one and has
	// no gaps. Thus, the series 10,11,12,13,14 is valid, but 10,11,13,14 is
	// not because there's a gap between 11 and 13. Also, 10,12,11,13 is not
	// valid because 12 and 11 are out of order.
	ErrOutOfOrder = errors.New("out of order")

	// ErrOutOfRange is returned from truncateFront() and truncateBack() when
	// the index not in the range of the log's first and last index. Or, this
	// may be returned when the caller is attempting to remove *all* entries;
	// The log requires that at least one entry exists following a truncate.
	ErrOutOfRange = errors.New("out of range")

	ErrInvalidSeek = errors.New("invalid seek")
)

const (
	storecount   = "raft.log_store.store_logs.count"
	storelatency = "raft.log_store.store_logs.latency_ms"

	deletecount   = "raft.log_store.delete_range.count"
	deletelatency = "raft.log_store.delete_range.latency_ms"
)

// LogStore is a write ahead Raft log
type LogStore struct {
	mu       sync.RWMutex
	opts     Options
	batch    batch       // batch reused for writing
	closed   bool        // whether or not log has been closed
	first    uint64      // index of the first entry in log
	last     uint64      // index of the last entry in log
	sfile    *LogFile    // tail segment file handle
	cache    []*raft.Log // in memory cache of all logs
	segments []*segment  // in memory file representation
}

// Options for Open
type Options struct {
	Path        string
	ArchivePath string
	SegmentSize int64
}

var DefaultOptions = Options{
	Path:        "/data/vol/raft/log",
	ArchivePath: "/data/vol/raft/archive",
	SegmentSize: 16 * 1024 * 1024, // 16 MB log segment files
}

// segment represents a single segment file
type segment struct {
	path  string // path of segment file
	index uint64 // first index of segment
	epos  []bpos
}

type bpos struct {
	pos int // byte position
	end int // one byte past pos
}

// batch of entries. used to write multiple entries at once using writeBatch()
type batch struct {
	data    []byte
	datas   []byte
	entries []entry
}

type entry struct {
	index uint64
	size  int
}

// write an entry to the batch
func (b *batch) write(index uint64, log *raft.Log) {
	b.data = b.data[:0]
	b.data = appendLog(b.data, log)
	b.entries = append(b.entries, entry{index, len(b.data)})
	b.datas = append(b.datas, b.data...)
}

// clear the batch for reuse.
func (b *batch) clear() {
	b.data = b.data[:0]
	b.datas = b.datas[:0]
	b.entries = b.entries[:0]
}

// Open the Raft log
func NewStore(opts Options) (*LogStore, error) {
	s := &LogStore{opts: opts}
	if err := s.Open(); err != nil {
		return nil, err
	}

	return s, nil
}

func (l *LogStore) Open() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	path, err := filepath.Abs(l.opts.Path)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(path, 0750); err != nil {
		return err
	}

	archive, err := filepath.Abs(l.opts.ArchivePath)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(archive, 0750); err != nil {
		return err
	}

	if err := l.load(); err != nil {
		return err
	}

	l.closed = false

	return nil
}

func (l *LogStore) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrClosed
	}

	l.closed = true

	if len(l.segments) == 0 {
		return nil
	}

	if err := l.sfile.Sync(); err != nil {
		return err
	}

	if err := l.sfile.Close(); err != nil {
		return err
	}

	l.cache = []*raft.Log{}
	l.segments = []*segment{}

	return nil
}

// FirstIndex returns the first known index from the Raft log.
func (l *LogStore) FirstIndex() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.firstIndex()
}

func (l *LogStore) firstIndex() (uint64, error) {
	if l.closed {
		return 0, ErrClosed
	}

	if l.last == 0 {
		return 0, nil
	}

	return l.first, nil
}

// LastIndex returns the last known index from the Raft log.
func (l *LogStore) LastIndex() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.lastIndex()
}

func (l *LogStore) lastIndex() (uint64, error) {
	if l.closed {
		return 0, ErrClosed
	}

	if l.last == 0 {
		return 0, nil
	}

	return l.last, nil
}

// GetLog is used to retrieve a log from the Log at a given index.
func (l *LogStore) GetLog(index uint64, log *raft.Log) error {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index < l.first || index > l.last {
		return raft.ErrLogNotFound
	}

	*log = *l.cache[index-l.first]

	return nil
}

func appendUvarint(dst []byte, x uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], x)
	dst = append(dst, buf[:n]...)
	return dst
}

func appendLog(dst []byte, log *raft.Log) []byte {
	dst = append(dst, byte(log.Type))
	dst = appendUvarint(dst, log.Term)
	dst = appendUvarint(dst, uint64(len(log.Data)))
	dst = append(dst, log.Data...)
	dst = appendUvarint(dst, uint64(len(log.Extensions)))
	dst = append(dst, log.Extensions...)
	return dst
}

// StoreLog is used to store a single raft log
func (l *LogStore) StoreLog(log *raft.Log) error {
	return l.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (l *LogStore) StoreLogs(logs []*raft.Log) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrClosed
	}

	if len(logs) == 0 {
		return nil
	}

	l.batch.clear()

	for _, log := range logs {
		l.batch.write(log.Index, log)
	}

	if err := l.writeBatch(&l.batch); err != nil {
		return err
	}

	l.cache = append(l.cache, logs...)

	return nil
}

// DeleteRange is used to delete logs within a given range inclusively.
func (l *LogStore) DeleteRange(min, max uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrClosed
	}

	if len(l.segments) == 0 {
		// nothing to delete
		return nil
	}

	first, err := l.firstIndex()
	if err != nil {
		return err
	}

	last, err := l.lastIndex()
	if err != nil {
		return err
	}

	if min == first && max == last {
		return l.truncateAll()
	} else if min == first {
		return l.truncateFront(max + 1)
	} else if max == last {
		return l.truncateBack(min - 1)
	} else {
		return ErrOutOfRange
	}
}

// load all the segments.
func (l *LogStore) load() error {
	fis, err := ioutil.ReadDir(l.opts.Path)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		name := fi.Name()
		if fi.IsDir() || len(name) != 32 {
			continue
		}

		if len(name) != 32 {
			continue
		}

		path := filepath.Join(l.opts.Path, name)
		s, chunk, err := loadSegmentAndLogsFromFile(path)
		if err != nil {
			return err
		}

		l.cache = append(l.cache, chunk...)
		l.segments = append(l.segments, s)
	}

	if len(l.segments) == 0 {
		return err
	}

	l.first = l.segments[0].index

	// Open the last segment for appending
	lseg := l.segments[len(l.segments)-1]
	l.sfile, err = OpenLogFile(lseg.path)
	if err != nil {
		return err
	}

	if _, err := l.sfile.Seek(0, 2); err != nil {
		return err
	}

	l.last = lseg.index + uint64(len(lseg.epos)) - 1
	return nil
}

func loadSegmentAndLogsFromFile(fp string) (*segment, []*raft.Log, error) {
	s := &segment{}
	logs := []*raft.Log{}

	file, err := os.Stat(fp)
	if err != nil {
		return nil, nil, err
	}

	name := file.Name()
	index, err := strconv.ParseUint(name[8:28], 10, 64)
	if err != nil || index == 0 {
		return nil, nil, err
	}

	data, err := ioutil.ReadFile(filepath.Clean(fp))
	if err != nil {
		return nil, nil, err
	}

	pos := 0
	epos := []bpos{}
	for len(data) > 0 {
		size, read, err := loadNextBinaryEntry(data)
		if err != nil {
			return nil, nil, err
		}

		if size == 0 && read == 1 {
			break
		}

		lhs := read
		rhs := size + read
		log := &raft.Log{}
		err = binaryToLog(data[lhs:rhs], index, log)
		if err != nil {
			return nil, nil, err
		}

		logs = append(logs, log)
		epos = append(epos, bpos{pos, pos + rhs})

		data = data[rhs:]
		index++
	}

	if len(logs) == 0 {
		return nil, nil, ErrCorrupt
	}

	s.path = fp
	s.index = logs[0].Index
	s.epos = epos

	return s, logs, nil
}

func binaryToLog(data []byte, index uint64, log *raft.Log) error {
	log.Index = index
	if len(data) == 0 {
		return ErrCorrupt
	}
	log.Type = raft.LogType(data[0])
	data = data[1:]
	var n int
	log.Term, n = binary.Uvarint(data)
	if n <= 0 {
		return ErrCorrupt
	}
	data = data[n:]
	size, n := binary.Uvarint(data)
	if n <= 0 {
		return ErrCorrupt
	}
	data = data[n:]
	if uint64(len(data)) < size {
		return ErrCorrupt
	}
	log.Data = data[:size]
	data = data[size:]
	size, n = binary.Uvarint(data)
	if n <= 0 {
		return ErrCorrupt
	}
	data = data[n:]
	if uint64(len(data)) < size {
		return ErrCorrupt
	}
	log.Extensions = data[:size]
	data = data[size:]
	if len(data) > 0 {
		return ErrCorrupt
	}
	return nil
}

// segmentName returns a textual representation of an index for
// lexical ordering. This is used for the file names of log segments.
func segmentName(index uint64) string {
	return fmt.Sprintf("raftlog.%020d.log", index)
}

// archiveName returns a textual representation of an index for
// lexical ordering. This is used for the file names of archived log segments.
func archiveName(index uint64) string {
	return fmt.Sprintf("raftlog.%020d.archive", index)
}

// Cycle the old segment for a new segment.
func (l *LogStore) cycle() error {
	if err := l.sfile.Sync(); err != nil {
		return err
	}

	if err := l.sfile.Close(); err != nil {
		return err
	}

	path := filepath.Join(l.opts.Path, segmentName(l.last+1))
	s := &segment{index: l.last + 1, path: path}

	var err error
	l.sfile, err = CreateLogFile(s.path, l.opts.SegmentSize)
	if err != nil {
		return err
	}

	err = l.sfile.Sync()
	if err != nil {
		return err
	}

	l.segments = append(l.segments, s)

	return nil
}

func appendBinaryEntry(dst []byte, data []byte) ([]byte, int) {
	// data_size + data
	pos := len(dst)
	dst = appendUvarint(dst, uint64(len(data)))
	dst = append(dst, data...)
	return dst, len(dst) - pos
}

// writeBatch writes the entries in the batch to the log in the order that they
// were added to the batch. The batch is cleared upon a successful return.
func (l *LogStore) writeBatch(b *batch) error {
	if len(l.segments) == 0 {
		// lazily create a first log
		index := b.entries[0].index
		path := filepath.Join(l.opts.Path, segmentName(index))
		seg := &segment{index: index, path: path}
		l.segments = append(l.segments, seg)
		l.first = index
		l.last = index - 1
		file, err := CreateLogFile(path, l.opts.SegmentSize)
		if err != nil {
			return err
		}

		l.sfile = file
	}

	// check that all indexes in batch are sane
	for i := 0; i < len(b.entries); i++ {
		if b.entries[i].index != l.last+uint64(i+1) {
			return ErrOutOfOrder
		}
	}

	// load the tail segment
	s := l.segments[len(l.segments)-1]
	size := 0
	if len(s.epos) > 0 {
		size = s.epos[len(s.epos)-1].end
	}
	if int64(size) > l.opts.SegmentSize {
		// tail segment has reached capacity. Close it and create a new one.
		if err := l.cycle(); err != nil {
			return err
		}
		s = l.segments[len(l.segments)-1]
	}

	pos := 0
	if len(s.epos) > 0 {
		pos = s.epos[len(s.epos)-1].end
	}

	ebuf := []byte{}
	datas := b.datas
	for i := 0; i < len(b.entries); i++ {
		data := datas[:b.entries[i].size]
		var n int
		ebuf, n = appendBinaryEntry(ebuf, data)
		s.epos = append(s.epos, bpos{pos, pos + n})
		datas = datas[b.entries[i].size:]
		pos += n
	}

	if _, err := l.sfile.Write(ebuf); err != nil {
		return err
	}
	l.last = b.entries[len(b.entries)-1].index

	if err := l.sfile.Sync(); err != nil {
		return err
	}

	return nil
}

// findSegment performs a bsearch on the segments
func (l *LogStore) findSegment(index uint64) int {
	i, j := 0, len(l.segments)
	for i < j {
		h := i + (j-i)/2
		if index >= l.segments[h].index {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

func loadNextBinaryEntry(data []byte) (int, int, error) {
	// data_size + data
	size, n := binary.Uvarint(data)
	if n <= 0 {
		return 0, 0, ErrCorrupt
	}
	if uint64(len(data)-n) < size {
		return 0, 0, ErrCorrupt
	}
	return int(size), n, nil
}

func (l *LogStore) loadSegment(index uint64) (*segment, error) {
	// check the last segment first.
	lseg := l.segments[len(l.segments)-1]
	if index >= lseg.index {
		return lseg, nil
	}

	// find in the segment array
	idx := l.findSegment(index)
	s := l.segments[idx]
	return s, nil
}

func (l *LogStore) truncateAll() error {
	for i := 0; i < len(l.segments); i++ {
		s := l.segments[i]
		archivePath := filepath.Join(l.opts.ArchivePath, archiveName(s.index))
		if err := os.Rename(s.path, archivePath); err != nil {
			return err
		}
	}

	l.segments = []*segment{}
	l.cache = []*raft.Log{}
	l.first, l.last = 0, 0

	return nil
}

// truncateFront truncates the front of the log by archiving all segments
// where segment.index < segment_index.
func (l *LogStore) truncateFront(index uint64) error {
	if index == 0 || l.last == 0 || index < l.first || index > l.last {
		return ErrOutOfRange
	}

	if index == l.first {
		// nothing to truncate
		return nil
	}

	segmentIndex := l.findSegment(index)

	// archive truncated segment files. archiving is handled by
	// another process which listens to files being moved into the ArchivePath
	for i := 0; i < segmentIndex; i++ {
		s := l.segments[i]
		archivePath := filepath.Join(l.opts.ArchivePath, archiveName(s.index))
		if err := os.Rename(s.path, archivePath); err != nil {
			return err
		}
	}

	l.segments = append([]*segment{}, l.segments[segmentIndex:]...)
	l.cache = l.cache[index-l.first:]
	l.first = index
	return nil
}

// truncateBack truncates the back of the log by
// deleting all segments where segment.index > segment_index.
// truncateBack partially truncates the last segment
// by deleting all entries where entry.index > index.
func (l *LogStore) truncateBack(index uint64) error {
	if index == 0 || l.last == 0 || index < l.first || index > l.last {
		return ErrOutOfRange
	}

	if index == l.last {
		// nothing to truncate
		return nil
	}

	segmentIndex := l.findSegment(index)

	s, err := l.loadSegment(index)
	if err != nil {
		return err
	}

	// Delete truncated segment files
	for i := segmentIndex + 1; i < len(l.segments); i++ {
		if err = os.Remove(l.segments[i].path); err != nil {
			return err
		}
	}

	pos := int64(s.epos[index-s.index].end)

	if err := l.sfile.Truncate(pos); err != nil {
		return err
	}

	n, err := l.sfile.Seek(pos, 0)
	if err != nil {
		return err
	}

	if n != pos {
		return ErrInvalidSeek
	}

	if err := l.sfile.Sync(); err != nil {
		return err
	}

	l.segments = append([]*segment{}, l.segments[:segmentIndex+1]...)
	l.cache = l.cache[:index-l.first+1]
	l.last = index

	s.epos = s.epos[:index-s.index+1]

	return nil
}
