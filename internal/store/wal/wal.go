package wal

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var ErrClosed = errors.New("WAL is closed")

type Encoding string

const (
	JSONEncoding   Encoding = "json"
	BinaryEncoding Encoding = "binary"
)

type Compression string

const (
	NoCompression   Compression = "none"
	GZIPCompression Compression = "gzip"
)

const (
	maxEntryBytes = 16 * 1024 * 1024
	fileMagicV1   = "KVSHARDWAL\x01"
	fileMagicV2   = "KVSHARDWAL\x02"
)

// Config controls persistence behavior. Use DefaultConfig as a starting point.
type Config struct {
	Encoding            Encoding
	Compression         Compression
	BatchSize           int
	BatchInterval       time.Duration
	AsyncCommit         bool
	CompactionThreshold int64
}

func DefaultConfig() Config {
	return Config{
		Encoding:            JSONEncoding,
		Compression:         NoCompression,
		BatchSize:           1,
		BatchInterval:       time.Second,
		CompactionThreshold: 1_000,
	}
}

type diskHeader struct {
	Encoding    Encoding    `json:"encoding"`
	Compression Compression `json:"compression"`
	Checksum    string      `json:"checksum"`
}

type WAL struct {
	mu            sync.Mutex
	file          *os.File
	writer        *bufio.Writer
	path          string
	config        Config
	dataOffset    int64
	version       int
	pending       int
	dirty         bool
	recordCount   int64
	backgroundErr error
	closed        bool
	stop          chan struct{}
	done          chan struct{}
	stopOnce      sync.Once
	logger        *slog.Logger
}

func New(ctx context.Context, shardID int, logger *slog.Logger) (*WAL, error) {
	return NewAtWithConfig(ctx, shardID, "data", DefaultConfig(), logger)
}

func NewWithEncoding(ctx context.Context, shardID int, encoding Encoding, logger *slog.Logger) (*WAL, error) {
	config := DefaultConfig()
	config.Encoding = encoding
	return NewAtWithConfig(ctx, shardID, "data", config, logger)
}

func NewAt(ctx context.Context, shardID int, dir string, logger *slog.Logger) (*WAL, error) {
	return NewAtWithConfig(ctx, shardID, dir, DefaultConfig(), logger)
}

func NewAtWithEncoding(ctx context.Context, shardID int, dir string, encoding Encoding, logger *slog.Logger) (*WAL, error) {
	config := DefaultConfig()
	config.Encoding = encoding
	return NewAtWithConfig(ctx, shardID, dir, config, logger)
}

func NewAtWithConfig(ctx context.Context, shardID int, dir string, config Config, logger *slog.Logger) (*WAL, error) {
	if ctx == nil {
		return nil, errors.New("context cannot be nil")
	}
	if shardID < 0 {
		return nil, errors.New("shard ID must be non-negative")
	}
	config, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create WAL directory: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("wal_%d.log", shardID))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	dataOffset, version, err := prepareFile(file, config)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("prepare WAL: %w", err)
	}
	w := &WAL{
		file:       file,
		writer:     bufio.NewWriter(file),
		path:       path,
		config:     config,
		dataOffset: dataOffset,
		version:    version,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
		logger:     logger.WithGroup("wal").With("shard", shardID),
	}
	go w.backgroundCommit(ctx)
	return w, nil
}

func normalizeConfig(config Config) (Config, error) {
	defaults := DefaultConfig()
	if config.Encoding == "" {
		config.Encoding = defaults.Encoding
	}
	if config.Compression == "" {
		config.Compression = defaults.Compression
	}
	if config.BatchSize == 0 {
		config.BatchSize = defaults.BatchSize
	}
	if config.BatchInterval == 0 {
		config.BatchInterval = defaults.BatchInterval
	}
	if config.Encoding != JSONEncoding && config.Encoding != BinaryEncoding {
		return Config{}, fmt.Errorf("unsupported WAL encoding %q", config.Encoding)
	}
	if config.Compression != NoCompression && config.Compression != GZIPCompression {
		return Config{}, fmt.Errorf("unsupported WAL compression %q", config.Compression)
	}
	if config.BatchSize < 1 {
		return Config{}, errors.New("WAL batch size must be greater than zero")
	}
	if config.BatchInterval < 0 {
		return Config{}, errors.New("WAL batch interval cannot be negative")
	}
	if config.CompactionThreshold < 0 {
		return Config{}, errors.New("WAL compaction threshold cannot be negative")
	}
	return config, nil
}

func (w *WAL) Path() string             { return w.path }
func (w *WAL) Encoding() Encoding       { return w.config.Encoding }
func (w *WAL) Compression() Compression { return w.config.Compression }

// Append writes a validated record. Synchronous mode fsyncs at each batch
// boundary. Async mode returns after the batch is written to the OS and lets
// the background committer perform fsync.
func (w *WAL) Append(entry Entry) error {
	if err := entry.validate(); err != nil {
		return err
	}
	record, err := w.encodeRecord(entry)
	if err != nil {
		return fmt.Errorf("encode WAL entry: %w", err)
	}
	if len(record) > maxEntryBytes {
		return fmt.Errorf("WAL entry exceeds %d bytes", maxEntryBytes)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.usableLocked(); err != nil {
		return err
	}
	if _, err := w.writer.Write(record); err != nil {
		return fmt.Errorf("append WAL entry: %w", err)
	}
	w.pending++
	w.dirty = true
	w.recordCount++
	if w.pending >= w.config.BatchSize {
		if err := w.flushLocked(!w.config.AsyncCommit); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) Replay(apply func(Entry) error) error {
	if apply == nil {
		return errors.New("WAL replay callback cannot be nil")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.usableLocked(); err != nil {
		return err
	}
	if err := w.flushLocked(true); err != nil {
		return fmt.Errorf("flush WAL before replay: %w", err)
	}
	if _, err := w.file.Seek(w.dataOffset, io.SeekStart); err != nil {
		return fmt.Errorf("seek WAL: %w", err)
	}
	count, err := w.replayLocked(apply)
	if err != nil {
		return err
	}
	w.recordCount = count
	_, err = w.file.Seek(0, io.SeekEnd)
	return err
}

func (w *WAL) replayLocked(apply func(Entry) error) (int64, error) {
	if w.version == 0 || (w.version == 1 && w.config.Encoding == JSONEncoding) {
		return replayLegacyJSON(w.file, apply)
	}
	if w.version == 1 {
		return replayLegacyBinary(w.file, apply)
	}
	return replayFramed(w.file, w.config, apply)
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.usableLocked(); err != nil {
		return err
	}
	return w.flushLocked(true)
}

func (w *WAL) Close() error {
	w.stopOnce.Do(func() { close(w.stop) })
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	flushErr := w.flushLocked(true)
	closeErr := w.file.Close()
	if flushErr != nil {
		return flushErr
	}
	if closeErr != nil {
		return fmt.Errorf("close WAL: %w", closeErr)
	}
	return w.backgroundErr
}

func (w *WAL) backgroundCommit(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.config.BatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if w.backgroundErr == nil && w.dirty {
				w.backgroundErr = w.flushLocked(true)
			}
			w.mu.Unlock()
		case <-ctx.Done():
			return
		case <-w.stop:
			return
		}
	}
}

func (w *WAL) usableLocked() error {
	if w.closed {
		return ErrClosed
	}
	if w.backgroundErr != nil {
		return fmt.Errorf("background WAL commit failed: %w", w.backgroundErr)
	}
	return nil
}

func (w *WAL) flushLocked(syncDisk bool) error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("flush WAL: %w", err)
	}
	w.pending = 0
	if syncDisk && w.dirty {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("sync WAL: %w", err)
		}
		w.dirty = false
	}
	return nil
}

// NeedsCompaction reports whether stale history is large enough to justify
// rewriting the WAL. A threshold of zero disables automatic compaction.
func (w *WAL) NeedsCompaction(liveKeys int64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	threshold := w.config.CompactionThreshold
	return threshold > 0 && w.recordCount >= threshold && w.recordCount > liveKeys*2
}

// Compact atomically replaces the WAL with the supplied live-state entries.
func (w *WAL) Compact(entries []Entry) error {
	for _, entry := range entries {
		if err := entry.validate(); err != nil {
			return err
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.usableLocked(); err != nil {
		return err
	}
	if err := w.flushLocked(true); err != nil {
		return err
	}

	dir := filepath.Dir(w.path)
	temp, err := os.CreateTemp(dir, ".wal-compact-*")
	if err != nil {
		return fmt.Errorf("create compacted WAL: %w", err)
	}
	tempPath := temp.Name()
	cleanup := func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}
	if err := temp.Chmod(0o600); err != nil {
		cleanup()
		return err
	}
	header, err := makeHeader(w.config)
	if err != nil {
		cleanup()
		return err
	}
	if _, err := temp.Write(header); err != nil {
		cleanup()
		return err
	}
	for _, entry := range entries {
		record, err := encodeV2Record(entry, w.config)
		if err != nil {
			cleanup()
			return err
		}
		if len(record) > maxEntryBytes {
			cleanup()
			return fmt.Errorf("WAL entry exceeds %d bytes", maxEntryBytes)
		}
		if _, err := temp.Write(record); err != nil {
			cleanup()
			return err
		}
	}
	if err := temp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tempPath, w.path); err != nil {
		cleanup()
		return fmt.Errorf("replace WAL during compaction: %w", err)
	}
	if _, err := temp.Seek(0, io.SeekEnd); err != nil {
		_ = temp.Close()
		return fmt.Errorf("seek compacted WAL: %w", err)
	}
	_ = w.file.Close()
	w.file = temp
	w.writer = bufio.NewWriter(temp)
	w.dataOffset = int64(len(header))
	w.version = 2
	w.pending = 0
	w.dirty = false
	w.recordCount = int64(len(entries))
	if directory, err := os.Open(dir); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}

func prepareFile(file *os.File, config Config) (int64, int, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, 0, err
	}
	if info.Size() == 0 {
		header, err := makeHeader(config)
		if err != nil {
			return 0, 0, err
		}
		if _, err := file.Write(header); err != nil {
			return 0, 0, err
		}
		if err := file.Sync(); err != nil {
			return 0, 0, err
		}
		return int64(len(header)), 2, nil
	}

	reader := bufio.NewReader(file)
	line, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return 0, 0, err
	}
	trimmed := bytes.TrimSuffix(line, []byte{'\n'})
	if bytes.HasPrefix(trimmed, []byte(fileMagicV2)) {
		var header diskHeader
		if err := json.Unmarshal(trimmed[len(fileMagicV2):], &header); err != nil {
			return 0, 0, fmt.Errorf("invalid WAL header: %w", err)
		}
		if header.Encoding != config.Encoding || header.Compression != config.Compression || header.Checksum != "crc32" {
			return 0, 0, fmt.Errorf("WAL format is encoding=%s compression=%s checksum=%s, requested encoding=%s compression=%s checksum=crc32", header.Encoding, header.Compression, header.Checksum, config.Encoding, config.Compression)
		}
		return int64(len(line)), 2, nil
	}
	if bytes.HasPrefix(trimmed, []byte(fileMagicV1)) {
		if config.Compression != NoCompression {
			return 0, 0, errors.New("legacy WAL does not support compression; compact it before enabling compression")
		}
		marker := byte('J')
		if config.Encoding == BinaryEncoding {
			marker = 'B'
		}
		if len(trimmed) != len(fileMagicV1)+1 || trimmed[len(fileMagicV1)] != marker {
			return 0, 0, fmt.Errorf("legacy WAL uses a different encoding than %q", config.Encoding)
		}
		return int64(len(line)), 1, nil
	}
	if config.Encoding != JSONEncoding || config.Compression != NoCompression {
		return 0, 0, errors.New("headerless WAL is legacy JSON and cannot use the requested format")
	}
	return 0, 0, nil
}

func makeHeader(config Config) ([]byte, error) {
	encoded, err := json.Marshal(diskHeader{Encoding: config.Encoding, Compression: config.Compression, Checksum: "crc32"})
	if err != nil {
		return nil, err
	}
	header := append([]byte(fileMagicV2), encoded...)
	return append(header, '\n'), nil
}

func (w *WAL) encodeRecord(entry Entry) ([]byte, error) {
	if w.version == 0 || (w.version == 1 && w.config.Encoding == JSONEncoding) {
		record, err := json.Marshal(entry)
		return append(record, '\n'), err
	}
	if w.version == 1 {
		return encodeLegacyBinary(entry), nil
	}
	return encodeV2Record(entry, w.config)
}

func encodeV2Record(entry Entry, config Config) ([]byte, error) {
	var payload []byte
	var err error
	if config.Encoding == JSONEncoding {
		payload, err = json.Marshal(entry)
	} else {
		payload = encodeBinaryPayload(entry)
	}
	if err != nil {
		return nil, err
	}
	if len(payload) > maxEntryBytes-8 {
		return nil, fmt.Errorf("WAL payload exceeds %d bytes", maxEntryBytes-8)
	}
	if config.Compression == GZIPCompression {
		payload, err = gzipBytes(payload)
		if err != nil {
			return nil, err
		}
	}
	record := make([]byte, 4+len(payload)+4)
	binary.BigEndian.PutUint32(record[:4], uint32(len(payload)))
	copy(record[4:], payload)
	binary.BigEndian.PutUint32(record[4+len(payload):], crc32.ChecksumIEEE(payload))
	return record, nil
}

func encodeBinaryPayload(entry Entry) []byte {
	key, value := []byte(entry.Key), []byte(entry.Value)
	payload := make([]byte, 17+len(key)+len(value))
	if entry.Operation == PutOperation {
		payload[0] = 1
	} else {
		payload[0] = 2
	}
	binary.BigEndian.PutUint64(payload[1:9], uint64(entry.TTL))
	binary.BigEndian.PutUint32(payload[9:13], uint32(len(key)))
	binary.BigEndian.PutUint32(payload[13:17], uint32(len(value)))
	copy(payload[17:], key)
	copy(payload[17+len(key):], value)
	return payload
}

func encodeLegacyBinary(entry Entry) []byte {
	payload := encodeBinaryPayload(entry)
	record := make([]byte, 4+len(payload)+4)
	binary.BigEndian.PutUint32(record[:4], uint32(len(payload)))
	copy(record[4:], payload)
	binary.BigEndian.PutUint32(record[4+len(payload):], crc32.ChecksumIEEE(payload))
	return record
}

func replayFramed(reader io.Reader, config Config, apply func(Entry) error) (int64, error) {
	var count int64
	for {
		payload, err := readFrame(reader, count+1)
		if err == io.EOF {
			return count, nil
		}
		if err != nil {
			return count, err
		}
		if config.Compression == GZIPCompression {
			payload, err = gunzipBytes(payload)
			if err != nil {
				return count, fmt.Errorf("decompress WAL record %d: %w", count+1, err)
			}
		}
		var entry Entry
		if config.Encoding == JSONEncoding {
			err = json.Unmarshal(payload, &entry)
		} else {
			entry, err = decodeBinaryPayload(payload)
		}
		if err != nil {
			return count, fmt.Errorf("decode WAL record %d: %w", count+1, err)
		}
		count++
		if err := applyEntry(entry, int(count), "record", apply); err != nil {
			return count, err
		}
	}
}

func readFrame(reader io.Reader, index int64) ([]byte, error) {
	var size uint32
	if err := binary.Read(reader, binary.BigEndian, &size); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("read WAL record %d size: %w", index, err)
	}
	if size == 0 || size > maxEntryBytes {
		return nil, fmt.Errorf("invalid WAL record %d size %d", index, size)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, fmt.Errorf("read WAL record %d: %w", index, err)
	}
	var checksum uint32
	if err := binary.Read(reader, binary.BigEndian, &checksum); err != nil {
		return nil, fmt.Errorf("read WAL record %d checksum: %w", index, err)
	}
	if crc32.ChecksumIEEE(payload) != checksum {
		return nil, fmt.Errorf("WAL record %d checksum mismatch", index)
	}
	return payload, nil
}

func replayLegacyJSON(reader io.Reader, apply func(Entry) error) (int64, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), maxEntryBytes+1)
	var count int64
	for scanner.Scan() {
		if len(scanner.Bytes()) == 0 {
			continue
		}
		count++
		var entry Entry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return count, fmt.Errorf("decode WAL line %d: %w", count, err)
		}
		if err := applyEntry(entry, int(count), "line", apply); err != nil {
			return count, err
		}
	}
	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("read WAL: %w", err)
	}
	return count, nil
}

func replayLegacyBinary(reader io.Reader, apply func(Entry) error) (int64, error) {
	var count int64
	for {
		payload, err := readFrame(reader, count+1)
		if err == io.EOF {
			return count, nil
		}
		if err != nil {
			return count, err
		}
		entry, err := decodeBinaryPayload(payload)
		if err != nil {
			return count, fmt.Errorf("decode WAL record %d: %w", count+1, err)
		}
		count++
		if err := applyEntry(entry, int(count), "record", apply); err != nil {
			return count, err
		}
	}
}

func decodeBinaryPayload(payload []byte) (Entry, error) {
	if len(payload) < 17 {
		return Entry{}, errors.New("binary payload is too short")
	}
	op := payload[0]
	ttl := int64(binary.BigEndian.Uint64(payload[1:9]))
	keyLen := binary.BigEndian.Uint32(payload[9:13])
	valueLen := binary.BigEndian.Uint32(payload[13:17])
	if uint64(keyLen)+uint64(valueLen) != uint64(len(payload)-17) {
		return Entry{}, errors.New("invalid key/value lengths")
	}
	operation := PutOperation
	if op == 2 {
		operation = DeleteOperation
	} else if op != 1 {
		return Entry{}, fmt.Errorf("unknown operation byte %d", op)
	}
	keyEnd := 17 + int(keyLen)
	return Entry{Operation: operation, Key: string(payload[17:keyEnd]), Value: string(payload[keyEnd:]), TTL: ttl}, nil
}

func gzipBytes(data []byte) ([]byte, error) {
	var output bytes.Buffer
	writer := gzip.NewWriter(&output)
	if _, err := writer.Write(data); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func gunzipBytes(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	decoded, err := io.ReadAll(io.LimitReader(reader, maxEntryBytes+1))
	if err != nil {
		return nil, err
	}
	if len(decoded) > maxEntryBytes {
		return nil, errors.New("decompressed WAL entry exceeds limit")
	}
	return decoded, nil
}

func applyEntry(entry Entry, index int, kind string, apply func(Entry) error) error {
	if err := entry.validate(); err != nil {
		return fmt.Errorf("invalid WAL %s %d: %w", kind, index, err)
	}
	if err := apply(entry); err != nil {
		return fmt.Errorf("apply WAL %s %d: %w", kind, index, err)
	}
	return nil
}
