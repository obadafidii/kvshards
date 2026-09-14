package wal

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func testLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestWALAppendSyncReplayAndReopen(t *testing.T) {
	dir := t.TempDir()
	w, err := NewAt(t.Context(), 7, dir, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	want := []Entry{
		{Operation: PutOperation, Key: "quoted", Value: "line\n\"two\"", TTL: 42},
		{Operation: DeleteOperation, Key: "quoted"},
	}
	for _, entry := range want {
		if err := w.Append(entry); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if info, err := os.Stat(filepath.Join(dir, "wal_7.log")); err != nil || info.Size() == 0 {
		t.Fatalf("WAL file stat = (%v, %v)", info, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewAt(t.Context(), 7, dir, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var got []Entry
	if err := reopened.Replay(func(entry Entry) error { got = append(got, entry); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Replay() = %#v, want %#v", got, want)
	}
}

func TestWALEncodingsRoundTrip(t *testing.T) {
	for _, encoding := range []Encoding{JSONEncoding, BinaryEncoding} {
		t.Run(string(encoding), func(t *testing.T) {
			dir := t.TempDir()
			w, err := NewAtWithEncoding(t.Context(), 4, dir, encoding, testLogger())
			if err != nil {
				t.Fatal(err)
			}
			want := []Entry{
				{Operation: PutOperation, Key: "unicode-🔑", Value: "line\nwith\x00bytes", TTL: -1},
				{Operation: DeleteOperation, Key: "unicode-🔑"},
			}
			for _, entry := range want {
				if err := w.Append(entry); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			reopened, err := NewAtWithEncoding(t.Context(), 4, dir, encoding, testLogger())
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if reopened.Encoding() != encoding {
				t.Fatalf("Encoding() = %q, want %q", reopened.Encoding(), encoding)
			}
			var got []Entry
			if err := reopened.Replay(func(entry Entry) error { got = append(got, entry); return nil }); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("Replay() = %#v, want %#v", got, want)
			}
		})
	}
}

func TestWALRejectsInvalidOrMismatchedEncoding(t *testing.T) {
	if _, err := NewAtWithEncoding(t.Context(), 0, t.TempDir(), "xml", testLogger()); err == nil {
		t.Fatal("NewAtWithEncoding(invalid) error = nil")
	}
	for _, original := range []Encoding{JSONEncoding, BinaryEncoding} {
		t.Run(string(original), func(t *testing.T) {
			dir := t.TempDir()
			w, err := NewAtWithEncoding(t.Context(), 0, dir, original, testLogger())
			if err != nil {
				t.Fatal(err)
			}
			_ = w.Append(Entry{Operation: PutOperation, Key: "key", Value: "value"})
			_ = w.Close()
			other := JSONEncoding
			if original == JSONEncoding {
				other = BinaryEncoding
			}
			if _, err := NewAtWithEncoding(t.Context(), 0, dir, other, testLogger()); err == nil {
				t.Fatalf("opening %s WAL as %s returned nil error", original, other)
			}
		})
	}
}

func TestBinaryWALDetectsChecksumCorruption(t *testing.T) {
	dir := t.TempDir()
	w, err := NewAtWithEncoding(t.Context(), 0, dir, BinaryEncoding, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Entry{Operation: PutOperation, Key: "key", Value: "value"}); err != nil {
		t.Fatal(err)
	}
	path := w.Path()
	offset := w.dataOffset
	_ = w.Close()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	contents[int(offset)+5] ^= 0xff
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	w, err = NewAtWithEncoding(t.Context(), 0, dir, BinaryEncoding, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Replay(func(Entry) error { return nil }); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("Replay(corrupt binary) error = %v", err)
	}
}

func TestWALReplaysLegacyHeaderlessJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal_9.log")
	if err := os.WriteFile(path, []byte("{\"operation\":\"put\",\"key\":\"legacy\",\"value\":\"works\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := NewAtWithEncoding(t.Context(), 9, dir, JSONEncoding, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var got Entry
	if err := w.Replay(func(entry Entry) error { got = entry; return nil }); err != nil {
		t.Fatal(err)
	}
	if got.Key != "legacy" || got.Value != "works" {
		t.Fatalf("legacy Replay() = %#v", got)
	}
}

func TestWALReplaysVersionOneFormats(t *testing.T) {
	entry := Entry{Operation: PutOperation, Key: "legacy", Value: "works", TTL: 42}
	for _, encoding := range []Encoding{JSONEncoding, BinaryEncoding} {
		t.Run(string(encoding), func(t *testing.T) {
			dir := t.TempDir()
			marker := byte('J')
			var record []byte
			if encoding == JSONEncoding {
				record, _ = json.Marshal(entry)
				record = append(record, '\n')
			} else {
				marker = 'B'
				record = encodeLegacyBinary(entry)
			}
			contents := append([]byte(fileMagicV1), marker, '\n')
			contents = append(contents, record...)
			if err := os.WriteFile(filepath.Join(dir, "wal_5.log"), contents, 0o600); err != nil {
				t.Fatal(err)
			}
			w, err := NewAtWithEncoding(t.Context(), 5, dir, encoding, testLogger())
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			var got Entry
			if err := w.Replay(func(replayed Entry) error { got = replayed; return nil }); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, entry) {
				t.Fatalf("Replay(v1 %s) = %#v, want %#v", encoding, got, entry)
			}
		})
	}
}

func TestWALRejectsCompressionMismatch(t *testing.T) {
	dir := t.TempDir()
	config := DefaultConfig()
	config.Compression = GZIPCompression
	w, err := NewAtWithConfig(t.Context(), 0, dir, config, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	config.Compression = NoCompression
	if _, err := NewAtWithConfig(t.Context(), 0, dir, config, testLogger()); err == nil {
		t.Fatal("opening gzip WAL without compression returned nil error")
	}
}

func TestWALValidation(t *testing.T) {
	w, err := NewAt(t.Context(), 0, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for name, entry := range map[string]Entry{
		"empty key":         {Operation: PutOperation},
		"unknown operation": {Operation: "rotate", Key: "key"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := w.Append(entry); err == nil {
				t.Fatal("Append() error = nil")
			}
		})
	}
	if err := w.Replay(nil); err == nil {
		t.Fatal("Replay(nil) error = nil")
	}
	if _, err := NewAt(t.Context(), -1, t.TempDir(), nil); err == nil {
		t.Fatal("NewAt(negative shard) error = nil")
	}
}

func TestWALCloseIsIdempotentAndRejectsOperations(t *testing.T) {
	w, err := NewAt(t.Context(), 0, t.TempDir(), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if err := w.Append(Entry{Operation: PutOperation, Key: "key"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append() error = %v, want ErrClosed", err)
	}
	if err := w.Sync(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Sync() error = %v, want ErrClosed", err)
	}
	if err := w.Replay(func(Entry) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("Replay() error = %v, want ErrClosed", err)
	}
}

func TestWALReplayReportsCorruptionAndCallbackErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal_1.log")
	if err := os.WriteFile(path, []byte("not-json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := NewAt(t.Context(), 1, dir, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Replay(func(Entry) error { return nil }); err == nil || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("Replay(corrupt) error = %v", err)
	}
	_ = w.Close()

	w, err = NewAt(t.Context(), 2, dir, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_ = w.Append(Entry{Operation: PutOperation, Key: "key"})
	want := errors.New("apply failed")
	if err := w.Replay(func(Entry) error { return want }); !errors.Is(err, want) {
		t.Fatalf("Replay(callback error) = %v", err)
	}
}

func TestWALReplaysEntriesLargerThanScannerDefault(t *testing.T) {
	dir := t.TempDir()
	w, err := NewAt(t.Context(), 0, dir, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Repeat("value", 20_000)
	if err := w.Append(Entry{Operation: PutOperation, Key: "large", Value: want}); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := w.Replay(func(entry Entry) error {
		got = entry.Value
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("large replay value length = %d, want %d", len(got), len(want))
	}
	_ = w.Close()
}

func TestWALRejectsOversizedEntry(t *testing.T) {
	w, err := NewAt(t.Context(), 0, t.TempDir(), testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	value := strings.Repeat("x", maxEntryBytes)
	if err := w.Append(Entry{Operation: PutOperation, Key: "too-large", Value: value}); err == nil {
		t.Fatal("Append(oversized) error = nil")
	}
}

func TestWALBatchingFlushesOnSizeAndInterval(t *testing.T) {
	t.Run("size", func(t *testing.T) {
		config := DefaultConfig()
		config.BatchSize = 3
		config.BatchInterval = time.Hour
		w, err := NewAtWithConfig(t.Context(), 0, t.TempDir(), config, testLogger())
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		headerBytes := w.dataOffset
		_ = w.Append(Entry{Operation: PutOperation, Key: "one", Value: "1"})
		_ = w.Append(Entry{Operation: PutOperation, Key: "two", Value: "2"})
		if info, _ := os.Stat(w.Path()); info.Size() != headerBytes {
			t.Fatalf("file size before full batch = %d, want %d", info.Size(), headerBytes)
		}
		_ = w.Append(Entry{Operation: PutOperation, Key: "three", Value: "3"})
		if info, _ := os.Stat(w.Path()); info.Size() <= headerBytes {
			t.Fatalf("full batch was not flushed; size = %d", info.Size())
		}
	})

	t.Run("interval", func(t *testing.T) {
		config := DefaultConfig()
		config.BatchSize = 100
		config.BatchInterval = 10 * time.Millisecond
		w, err := NewAtWithConfig(t.Context(), 0, t.TempDir(), config, testLogger())
		if err != nil {
			t.Fatal(err)
		}
		defer w.Close()
		headerBytes := w.dataOffset
		_ = w.Append(Entry{Operation: PutOperation, Key: "one", Value: "1"})
		deadline := time.Now().Add(time.Second)
		for {
			info, _ := os.Stat(w.Path())
			if info.Size() > headerBytes {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("batch interval did not flush pending entry")
			}
			time.Sleep(time.Millisecond)
		}
	})
}

func TestWALAsyncCommitDefersFsyncUntilExplicitSync(t *testing.T) {
	config := DefaultConfig()
	config.AsyncCommit = true
	config.BatchInterval = time.Hour
	w, err := NewAtWithConfig(t.Context(), 0, t.TempDir(), config, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Append(Entry{Operation: PutOperation, Key: "key", Value: "value"}); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	dirtyBeforeSync := w.dirty
	w.mu.Unlock()
	if !dirtyBeforeSync {
		t.Fatal("async append unexpectedly marked record as disk-synced")
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	dirtyAfterSync := w.dirty
	w.mu.Unlock()
	if dirtyAfterSync {
		t.Fatal("Sync() left async WAL dirty")
	}
}

func TestWALGZIPCompressionRoundTripAndReducesSize(t *testing.T) {
	value := strings.Repeat("highly-compressible-value-", 10_000)
	sizes := make(map[Compression]int64)
	for _, compression := range []Compression{NoCompression, GZIPCompression} {
		dir := t.TempDir()
		config := DefaultConfig()
		config.Compression = compression
		w, err := NewAtWithConfig(t.Context(), 0, dir, config, testLogger())
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Append(Entry{Operation: PutOperation, Key: "key", Value: value}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		info, _ := os.Stat(w.Path())
		sizes[compression] = info.Size()
		reopened, err := NewAtWithConfig(t.Context(), 0, dir, config, testLogger())
		if err != nil {
			t.Fatal(err)
		}
		var got Entry
		if err := reopened.Replay(func(entry Entry) error { got = entry; return nil }); err != nil {
			t.Fatal(err)
		}
		_ = reopened.Close()
		if got.Value != value {
			t.Fatalf("compressed round trip value length = %d, want %d", len(got.Value), len(value))
		}
	}
	if sizes[GZIPCompression] >= sizes[NoCompression] {
		t.Fatalf("gzip WAL size %d is not smaller than uncompressed %d", sizes[GZIPCompression], sizes[NoCompression])
	}
}

func TestBinaryWALGZIPRoundTrip(t *testing.T) {
	config := DefaultConfig()
	config.Encoding = BinaryEncoding
	config.Compression = GZIPCompression
	dir := t.TempDir()
	w, err := NewAtWithConfig(t.Context(), 0, dir, config, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	want := Entry{Operation: PutOperation, Key: "binary", Value: strings.Repeat("value", 1_000), TTL: 99}
	if err := w.Append(want); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	w, err = NewAtWithConfig(t.Context(), 0, dir, config, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var got Entry
	if err := w.Replay(func(entry Entry) error { got = entry; return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("binary gzip Replay() = %#v, want %#v", got, want)
	}
}

func TestWALCompactionRewritesOnlyLiveEntries(t *testing.T) {
	config := DefaultConfig()
	config.CompactionThreshold = 3
	w, err := NewAtWithConfig(t.Context(), 0, t.TempDir(), config, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	for i := range 8 {
		_ = w.Append(Entry{Operation: PutOperation, Key: "key", Value: strings.Repeat("x", i+1)})
	}
	before, _ := os.Stat(w.Path())
	if !w.NeedsCompaction(1) {
		t.Fatal("NeedsCompaction(1) = false for stale-heavy WAL")
	}
	want := Entry{Operation: PutOperation, Key: "key", Value: "final"}
	if err := w.Compact([]Entry{want}); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(w.Path())
	if after.Size() >= before.Size() {
		t.Fatalf("compacted size %d is not smaller than %d", after.Size(), before.Size())
	}
	if w.NeedsCompaction(1) {
		t.Fatal("freshly compacted WAL still needs compaction")
	}
	var got []Entry
	if err := w.Replay(func(entry Entry) error { got = append(got, entry); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []Entry{want}) {
		t.Fatalf("compacted Replay() = %#v", got)
	}
	_ = w.Close()
}

func TestWALConfigValidation(t *testing.T) {
	tests := map[string]Config{
		"encoding":             {Encoding: "xml"},
		"compression":          {Encoding: JSONEncoding, Compression: "brotli"},
		"batch size":           {Encoding: JSONEncoding, Compression: NoCompression, BatchSize: -1},
		"batch interval":       {Encoding: JSONEncoding, Compression: NoCompression, BatchSize: 1, BatchInterval: -1},
		"compaction threshold": {Encoding: JSONEncoding, Compression: NoCompression, BatchSize: 1, BatchInterval: time.Second, CompactionThreshold: -1},
	}
	for name, config := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := NewAtWithConfig(t.Context(), 0, t.TempDir(), config, testLogger()); err == nil {
				t.Fatal("NewAtWithConfig() error = nil")
			}
		})
	}
}
