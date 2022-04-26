package logstore

import (
	"io/ioutil"
	"math"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func CreateLogFile(name string, size int64) (*LogFile, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}

	err = f.Truncate(size)
	if err != nil {
		return nil, err
	}

	logfile := &LogFile{file: f}

	return logfile, logfile.Mmap()
}

func OpenLogFile(name string) (*LogFile, error) {
	f, err := os.OpenFile(name, os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	logfile := &LogFile{file: f}

	return logfile, logfile.Mmap()
}

func ReadLogFile(name string) ([]byte, error) {
	return ioutil.ReadFile(filepath.Clean(name))
}

type LogFile struct {
	file   *os.File
	mmap   []byte
	cursor int64
}

func (f *LogFile) Close() error {
	err := unix.Munmap(f.mmap)
	if err != nil {
		return err
	}

	f.mmap = nil

	return f.file.Close()
}

func (f *LogFile) Write(b []byte) (int, error) {
	size := int64(len(b))
	filesize := int64(len(f.mmap))
	if f.cursor+size >= filesize {
		pagesize := int64(os.Getpagesize())
		factor := int64(math.Ceil(float64(size) / float64(pagesize)))
		if err := f.Truncate(filesize + (factor * pagesize)); err != nil {
			return 0, err
		}
		if err := f.Mmap(); err != nil {
			return 0, err
		}
	}

	copy(f.mmap[f.cursor:f.cursor+size], b)

	f.cursor += int64(len(b))

	return len(b), nil
}

func (f *LogFile) Sync() error {
	return unix.Msync(f.mmap, unix.MS_SYNC)
}

func (f *LogFile) Seek(offset int64, whence int) (int64, error) {
	cursor := f.cursor
	size := int64(len(f.mmap))
	switch whence {
	case 0:
		cursor = offset
	case 1:
		cursor = f.cursor + offset
	case 2:
		cursor = size + offset
	}

	var err error
	if cursor < 0 || cursor > size {
		cursor, err = f.file.Seek(offset, whence)
		if err != nil {
			return 0, err
		}
	}

	f.cursor = cursor
	return f.cursor, err
}

func (f *LogFile) Truncate(size int64) error {
	if size < 0 || size > int64(len(f.mmap)) {
		return f.file.Truncate(size)
	}

	if size < f.cursor {
		for i := size; i < f.cursor; i++ {
			f.mmap[i] = '\x00'
		}

		return nil
	}

	return nil
}

func (f *LogFile) Mmap() error {
	if f.mmap != nil {
		err := unix.Munmap(f.mmap)
		if err != nil {
			return err
		}
	}

	stat, err := f.file.Stat()
	if err != nil {
		return err
	}

	flags := unix.MAP_SHARED
	prot := unix.PROT_READ | unix.PROT_WRITE
	b, err := unix.Mmap(int(f.file.Fd()), 0, int(stat.Size()), prot, flags)
	if err != nil {
		return err
	}

	f.mmap = b

	return nil
}
