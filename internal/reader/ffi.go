// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package reader

// Minimal dlopen binding to the sd-journal API of libsystemd — the
// same runtime dependency python-systemd has today. The binding is
// hand-written instead of using github.com/coreos/go-systemd/v22/sdjournal
// for two reasons discovered during the Task 15 spike (spec risk 3):
//
//  1. go-systemd's sdjournal embeds <systemd/sd-journal.h> via cgo, so
//     any build machine needs the systemd development headers installed
//     (they are absent on this Fedora host, and CI runners would need a
//     package install). This binding needs only glibc's dlfcn.h and
//     resolves every symbol at RUNTIME from libsystemd.so.0 — the same
//     dlopen pattern go-systemd itself uses for the symbols.
//  2. go-systemd has no namespace support (sd_journal_open_namespace),
//     which the plan's open parameter list requires.
//
// The prototypes are hand-declared from the sd-journal ABI, which is
// stable: negative return values are -errno, and the constants
// (flags, wait events) are pinned in reader.go.
//
// Error channeling: every method returns (result..., rc, err) where
// err is non-nil ONLY for infrastructure failures (dlopen/dlsym —
// reported verbatim, naming the missing symbol), and rc carries the
// raw libsystemd return (negative = -errno) for the sdjournal layer to
// classify. A zero rc with a non-nil err is never a successful call.

/*
#cgo LDFLAGS: -ldl
#include <dlfcn.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

typedef struct sd_journal sd_journal;

// sd_id128_t is a 16-byte value type; the Go-side mirror keeps the
// same size and 8-byte alignment so by-value ABI passing matches.
typedef struct { uint64_t q0, q1; } go_id128_t;

typedef int      (*fn_open)(sd_journal**, int);
typedef int      (*fn_open_namespace)(sd_journal**, const char*, int);
typedef int      (*fn_open_directory)(sd_journal**, const char*, int);
typedef int      (*fn_open_files)(sd_journal**, const char**, int);
typedef int      (*fn_add_match)(sd_journal*, const void*, unsigned);
typedef int      (*fn_step)(sd_journal*);
typedef int      (*fn_seek_head)(sd_journal*);
typedef int      (*fn_seek_tail)(sd_journal*);
typedef int      (*fn_seek_cursor)(sd_journal*, const char*);
typedef int      (*fn_seek_monotonic)(sd_journal*, go_id128_t, uint64_t);
typedef int      (*fn_id128_get_boot)(go_id128_t*);
typedef int      (*fn_wait)(sd_journal*, uint64_t);
typedef int      (*fn_enumerate_data)(sd_journal*, const void**, size_t*);
typedef void     (*fn_restart_data)(sd_journal*);
typedef int      (*fn_get_cursor)(sd_journal*, char**);
typedef int      (*fn_get_fd)(sd_journal*);
typedef int      (*fn_get_realtime)(sd_journal*, uint64_t*);
typedef int      (*fn_has_persistent)(sd_journal*);
typedef int      (*fn_has_runtime)(sd_journal*);
typedef void     (*fn_close)(sd_journal*);

static void* go_dlsym(void* handle, const char* name) { return dlsym(handle, name); }

// call_enumerate_data copies the next data tuple into a caller buffer:
// returning the libsystemd pointer across the boundary would need a
// uintptr round-trip the cgo pointer checker flags. The sd-journal data
// threshold is 64 KiB, so a 1 MiB buffer never overflows.

static int call_open(void* f, sd_journal** out, int flags) { return ((fn_open)f)(out, flags); }
static int call_open_namespace(void* f, sd_journal** out, const char* ns, int flags) { return ((fn_open_namespace)f)(out, ns, flags); }
static int call_open_directory(void* f, sd_journal** out, const char* path, int flags) { return ((fn_open_directory)f)(out, path, flags); }

// call_open_files builds a NULL-terminated argv-style path array on the
// C side (libsystemd has no sd_journal_add_file export; files-mode is
// sd_journal_open_files).
static int call_open_files(void* f, sd_journal** out, int flags, int nfiles, char** files) {
	const char** argv = NULL;
	if (nfiles > 0) {
		argv = calloc((size_t)nfiles + 1, sizeof(char*));
		if (argv == NULL) { return -12; } // -ENOMEM
		for (int i = 0; i < nfiles; i++) { argv[i] = files[i]; }
	}
	int rc = ((fn_open_files)f)(out, argv, flags);
	free(argv);
	return rc;
}
static int call_add_match(void* f, sd_journal* j, const void* data, unsigned len) { return ((fn_add_match)f)(j, data, len); }

// sd_journal_next/previous return an INT: >0 the read position moved,
// 0 the end was reached, negative -errno. Declaring them uint64 (as an
// earlier draft did) zero-extends a negative rc into a huge positive
// and silently turns errors into "entry present".
static int call_step(void* f, sd_journal* j) { return ((fn_step)f)(j); }
static int call_seek_head(void* f, sd_journal* j) { return ((fn_seek_head)f)(j); }
static int call_seek_tail(void* f, sd_journal* j) { return ((fn_seek_tail)f)(j); }
static int call_seek_cursor(void* f, sd_journal* j, const char* c) { return ((fn_seek_cursor)f)(j, c); }
static int call_seek_monotonic(void* f, sd_journal* j, go_id128_t boot, uint64_t usec) { return ((fn_seek_monotonic)f)(j, boot, usec); }
static int call_id128_get_boot(void* f, go_id128_t* out) { return ((fn_id128_get_boot)f)(out); }
static int call_wait(void* f, sd_journal* j, uint64_t usec) { return ((fn_wait)f)(j, usec); }
static int call_enumerate_data(void* f, sd_journal* j, void* buf, size_t buflen, size_t* outlen) {
	const void* p = NULL;
	size_t l = 0;
	int rc = ((fn_enumerate_data)f)(j, &p, &l);
	if (rc == 1) {
		if (l > buflen) { return -75; } // -EOVERFLOW
		memcpy(buf, p, l);
		*outlen = l;
	}
	return rc;
}
static int call_get_cursor(void* f, sd_journal* j, char** out) { return ((fn_get_cursor)f)(j, out); }
static int call_get_fd(void* f, sd_journal* j) { return ((fn_get_fd)f)(j); }
static int call_get_realtime(void* f, sd_journal* j, uint64_t* out) { return ((fn_get_realtime)f)(j, out); }
static int call_has_persistent(void* f, sd_journal* j) { return ((fn_has_persistent)f)(j); }
static int call_has_runtime(void* f, sd_journal* j) { return ((fn_has_runtime)f)(j); }
static void call_close(void* f, sd_journal* j) { ((fn_close)f)(j); }

// jp_api is the resolved function-pointer table for the batch read loop.
// Resolved once (jp_api_init) instead of per call, deleting the per-call
// dlsym map+mutex lookup that the profile charged ~0.51 s to.
typedef struct {
	fn_step         next;
	fn_step         previous;
	fn_restart_data restart_data;
	fn_enumerate_data enumerate_data;
	fn_get_realtime get_realtime;
	fn_get_cursor   get_cursor;
	fn_seek_cursor  seek_cursor;
} jp_api;

static void jp_api_init(jp_api* a, void* next, void* previous, void* restart_data,
                        void* enumerate_data, void* get_realtime, void* get_cursor,
                        void* seek_cursor) {
	a->next = (fn_step)next;
	a->previous = (fn_step)previous;
	a->restart_data = (fn_restart_data)restart_data;
	a->enumerate_data = (fn_enumerate_data)enumerate_data;
	a->get_realtime = (fn_get_realtime)get_realtime;
	a->get_cursor = (fn_get_cursor)get_cursor;
	a->seek_cursor = (fn_seek_cursor)seek_cursor;
}

static void jp_put_u16(uint8_t* p, uint16_t v) { p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); }
static void jp_put_u32(uint8_t* p, uint32_t v) { p[0]=(uint8_t)v; p[1]=(uint8_t)(v>>8); p[2]=(uint8_t)(v>>16); p[3]=(uint8_t)(v>>24); }
static void jp_put_u64(uint8_t* p, uint64_t v) { for (int i=0;i<8;i++) { p[i]=(uint8_t)(v>>(8*i)); } }

// jp_read_batch reads up to max_entries entries, each atomic, into buf.
// It returns the number of entry records written; *used is the bytes
// written; *status is 0 (buffer full or max_entries reached), 1 (end of
// journal) or a negative -errno from the call that failed.
//
// PER-ENTRY LAYOUT (little-endian):
//   u64 realtime_usec
//   u16 cursor_len, cursor bytes
//   u32 nfields, then per field: u32 len, bytes (KEY=value)
//
// On a buffer overflow mid-entry the bytes of that entry are discarded and
// the position is rewound with sd_journal_seek_cursor to just BEFORE the
// entry (the cursor is read up front and kept for this), so a retry with a
// larger buffer re-reads it. A mid-entry libsystemd error keeps the
// fully-written entries, rewinds the partial one and leaves the position
// before the failing entry.
static int jp_read_batch(const jp_api* api, sd_journal* j, uint8_t* buf, size_t buflen,
                         int max_entries, size_t* used_out, int* status) {
	size_t used = 0;
	int count = 0;
	*status = 0;
	while (count < max_entries) {
		int rc = api->next(j);
		if (rc == 0) { *status = 1; break; }
		if (rc < 0) { *status = rc; break; }
		size_t entry_start = used;
		uint64_t rt = 0;
		api->get_realtime(j, &rt); // a failure leaves rt zero, as today's zero-value would
		char* cursor = NULL;
		api->get_cursor(j, &cursor);
		size_t clen = cursor ? strlen(cursor) : 0;
		if (clen > 0xffff) { clen = 0xffff; }
		// Prologue: u64 realtime + u16 cursor_len + cursor + u32 nfields.
		if (used + 10 + clen + 4 > buflen) {
			if (cursor) { api->seek_cursor(j, cursor); }
			else { api->previous(j); }
			free(cursor);
			used = entry_start;
			break;
		}
		size_t hdr = used;
		used += 10;
		jp_put_u64(buf + hdr, rt);
		jp_put_u16(buf + hdr + 8, (uint16_t)clen);
		if (clen) { memcpy(buf + used, cursor, clen); }
		used += clen;
		size_t nfields_pos = used;
		used += 4;
		uint32_t nfields = 0;
		api->restart_data(j);
		int failed = 0;
		for (;;) {
			const void* p = NULL;
			size_t l = 0;
			rc = api->enumerate_data(j, &p, &l);
			if (rc == 0) { break; }
			if (rc < 0) { failed = 2; *status = rc; break; }
			if (used + 4 + l > buflen) { failed = 1; break; }
			jp_put_u32(buf + used, (uint32_t)l);
			used += 4;
			memcpy(buf + used, p, l);
			used += l;
			nfields++;
		}
		if (failed) {
			if (cursor) { api->seek_cursor(j, cursor); }
			else { api->previous(j); }
			free(cursor);
			used = entry_start;
			break;
		}
		free(cursor);
		jp_put_u32(buf + nfields_pos, nfields);
		count++;
	}
	*used_out = used;
	return count;
}
*/
import "C"

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"unsafe"
)

// libNames mirrors go-systemd's probe list: pre-209 builds have a
// separate libsystemd-journal.
var libNames = []string{
	"libsystemd-journal.so.0",
	"libsystemd-journal.so",
	"libsystemd.so.0",
	"libsystemd.so",
}

var (
	ffiMu   sync.Mutex
	ffiLib  unsafe.Pointer
	ffiSyms = map[string]unsafe.Pointer{}
)

// ffiSymbol resolves one libsystemd symbol, dlopening the library on
// first use. Safe for concurrent use.
func ffiSymbol(name string) (unsafe.Pointer, error) {
	ffiMu.Lock()
	defer ffiMu.Unlock()
	if ffiLib == nil {
		var handle unsafe.Pointer
		var lastErr error
		for _, lib := range libNames {
			clib := C.CString(lib)
			handle = C.dlopen(clib, C.RTLD_LAZY|C.RTLD_GLOBAL)
			C.free(unsafe.Pointer(clib))
			if handle != nil {
				break
			}
			lastErr = errors.New("dlopen failed")
		}
		if handle == nil {
			return nil, fmt.Errorf("cannot dlopen the systemd journal library (tried %s): %w",
				strings.Join(libNames, ", "), lastErr)
		}
		ffiLib = handle
	}
	if p, ok := ffiSyms[name]; ok {
		return p, nil
	}
	cname := C.CString(name)
	p := C.go_dlsym(ffiLib, cname)
	C.free(unsafe.Pointer(cname))
	if p == nil {
		return nil, fmt.Errorf("libsystemd is missing symbol %s", name)
	}
	ffiSyms[name] = p
	return p, nil
}

// ffiJournal wraps one sd_journal handle. Methods return the raw
// libsystemd return code alongside an infrastructure error (see the
// package comment); the sdjournal layer classifies negative rc values
// via sdErr.
type ffiJournal struct {
	j *C.sd_journal
}

func ffiOpen(flags int) (*ffiJournal, int, error) {
	f, err := ffiSymbol("sd_journal_open")
	if err != nil {
		return nil, 0, err
	}
	var j *C.sd_journal
	rc := C.call_open(f, &j, C.int(flags)) //nolint:gocritic // false positive on cgo-generated pointer check
	if rc != 0 {
		return nil, int(rc), nil
	}
	return &ffiJournal{j: j}, 0, nil
}

func ffiOpenNamespace(namespace string, flags int) (*ffiJournal, int, error) {
	f, err := ffiSymbol("sd_journal_open_namespace")
	if err != nil {
		return nil, 0, err
	}
	cns := C.CString(namespace)
	defer C.free(unsafe.Pointer(cns))
	var j *C.sd_journal
	rc := C.call_open_namespace(f, &j, cns, C.int(flags)) //nolint:gocritic // false positive on cgo-generated pointer check
	if rc != 0 {
		return nil, int(rc), nil
	}
	return &ffiJournal{j: j}, 0, nil
}

func ffiOpenDirectory(path string, flags int) (*ffiJournal, int, error) {
	f, err := ffiSymbol("sd_journal_open_directory")
	if err != nil {
		return nil, 0, err
	}
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	var j *C.sd_journal
	rc := C.call_open_directory(f, &j, cpath, C.int(flags)) //nolint:gocritic // false positive on cgo-generated pointer check
	if rc != 0 {
		return nil, int(rc), nil
	}
	return &ffiJournal{j: j}, 0, nil
}

func ffiOpenFiles(files []string, flags int) (*ffiJournal, int, error) {
	f, err := ffiSymbol("sd_journal_open_files")
	if err != nil {
		return nil, 0, err
	}
	var argv []*C.char
	for _, file := range files {
		cfile := C.CString(file)
		defer C.free(unsafe.Pointer(cfile))
		argv = append(argv, cfile)
	}
	var j *C.sd_journal
	var argvPtr **C.char
	if len(argv) > 0 {
		argvPtr = &argv[0]
	}
	rc := int(C.call_open_files(f, &j, C.int(flags), C.int(len(argv)), argvPtr)) //nolint:gocritic // false positive on cgo-generated pointer check
	if rc != 0 {
		return nil, rc, nil
	}
	return &ffiJournal{j: j}, 0, nil
}

func (h *ffiJournal) addMatch(match string) (int, error) {
	f, err := ffiSymbol("sd_journal_add_match")
	if err != nil {
		return 0, err
	}
	cdata := C.CBytes([]byte(match))
	defer C.free(cdata)
	return int(C.call_add_match(f, h.j, cdata, C.uint(len(match)))), nil
}

// next steps to the next entry: rc > 0 the position moved, rc == 0 the
// end was reached, rc < 0 -errno (int ABI — see call_step).
func (h *ffiJournal) next() (int, error) {
	f, err := ffiSymbol("sd_journal_next")
	if err != nil {
		return 0, err
	}
	return int(C.call_step(f, h.j)), nil
}

func (h *ffiJournal) previous() (int, error) {
	f, err := ffiSymbol("sd_journal_previous")
	if err != nil {
		return 0, err
	}
	return int(C.call_step(f, h.j)), nil
}

func (h *ffiJournal) seekHead() (int, error) {
	f, err := ffiSymbol("sd_journal_seek_head")
	if err != nil {
		return 0, err
	}
	return int(C.call_seek_head(f, h.j)), nil
}

func (h *ffiJournal) seekTail() (int, error) {
	f, err := ffiSymbol("sd_journal_seek_tail")
	if err != nil {
		return 0, err
	}
	return int(C.call_seek_tail(f, h.j)), nil
}

func (h *ffiJournal) seekCursor(cursor string) (int, error) {
	f, err := ffiSymbol("sd_journal_seek_cursor")
	if err != nil {
		return 0, err
	}
	cc := C.CString(cursor)
	defer C.free(unsafe.Pointer(cc))
	return int(C.call_seek_cursor(f, h.j, cc)), nil
}

// id128 mirrors sd_id128_t for by-value ABI passing.
type id128 struct{ q0, q1 uint64 }

func (h *ffiJournal) seekMonotonicUsec(boot id128, usec uint64) (int, error) {
	f, err := ffiSymbol("sd_journal_seek_monotonic_usec")
	if err != nil {
		return 0, err
	}
	return int(C.call_seek_monotonic(f, h.j, C.go_id128_t{q0: C.uint64_t(boot.q0), q1: C.uint64_t(boot.q1)}, C.uint64_t(usec))), nil
}

func bootID() (id128, int, error) {
	f, err := ffiSymbol("sd_id128_get_boot")
	if err != nil {
		return id128{}, 0, err
	}
	var out C.go_id128_t
	rc := C.call_id128_get_boot(f, &out)
	return id128{q0: uint64(out.q0), q1: uint64(out.q1)}, int(rc), nil
}

// getFd registers the journal's notification fd and returns it (>= 0) or
// -errno. libsystemd's sd_journal_get_fd calls sd_journal_process
// internally, so it ABSORBS the open-time file-snapshot INVALIDATE: the
// first subsequent sd_journal_wait reports NOP for an unchanged journal
// while a genuine rotation/deletion after the call still reports
// INVALIDATE. This is the notification-tracking initialization Python
// gets from python-systemd's Reader.fileno() when the poller registers
// the reader (journalpump.py:1090-1100, register_for_poll).
func (h *ffiJournal) getFd() (int, error) {
	f, err := ffiSymbol("sd_journal_get_fd")
	if err != nil {
		return 0, err
	}
	return int(C.call_get_fd(f, h.j)), nil
}

// waitUsec blocks up to timeoutUsec; (1<<63)-1 waits indefinitely.
// Returns SD_JOURNAL_NOP/APPEND/INVALIDATE.
func (h *ffiJournal) waitUsec(timeoutUsec uint64) (int, error) {
	f, err := ffiSymbol("sd_journal_wait")
	if err != nil {
		return 0, err
	}
	return int(C.call_wait(f, h.j, C.uint64_t(timeoutUsec))), nil
}

// enumBufPool backs enumerateData: the C copy target (1 MiB, twice the
// 64 KiB sd-journal data threshold, so the -EOVERFLOW path is dead in
// practice).
//
// Still LIVE production code: enumerateData feeds materialize, which the
// positioning path (sdjournal.go applyInitial/Seek, via readNext) uses to
// land on the resume cursor and the tail. Task 32 step 7 (option B) left
// it in place deliberately — rewiring those parity-sensitive positioning
// paths onto ReadRaw is a possible follow-up, not part of this task.
var enumBufPool = sync.Pool{
	New: func() any { b := make([]byte, 1<<20); return &b },
}

// enumerateData returns the next data tuple of the current entry; rc
// is 1 on success, 0 at the end of the enumeration, or -errno.
func (h *ffiJournal) enumerateData() ([]byte, int, error) {
	f, err := ffiSymbol("sd_journal_enumerate_data")
	if err != nil {
		return nil, 0, err
	}
	bufPtr := enumBufPool.Get().(*[]byte)
	buf := *bufPtr
	defer func() { enumBufPool.Put(bufPtr) }()
	var length C.size_t
	rc := int(C.call_enumerate_data(f, h.j, unsafe.Pointer(&buf[0]), C.size_t(len(buf)), &length))
	if rc != 1 {
		return nil, rc, nil
	}
	return append([]byte(nil), buf[:length]...), rc, nil
}

func (h *ffiJournal) getCursor() (string, int, error) {
	f, err := ffiSymbol("sd_journal_get_cursor")
	if err != nil {
		return "", 0, err
	}
	var out *C.char
	rc := int(C.call_get_cursor(f, h.j, &out)) //nolint:gocritic // false positive on cgo-generated pointer check
	if rc != 0 {
		return "", rc, nil
	}
	s := C.GoString(out)
	C.free(unsafe.Pointer(out))
	return s, 0, nil
}

func (h *ffiJournal) realtimeUsec() (uint64, int, error) {
	f, err := ffiSymbol("sd_journal_get_realtime_usec")
	if err != nil {
		return 0, 0, err
	}
	var out C.uint64_t
	rc := int(C.call_get_realtime(f, h.j, &out))
	return uint64(out), rc, nil
}

func (h *ffiJournal) hasPersistentFiles() (int, error) {
	f, err := ffiSymbol("sd_journal_has_persistent_files")
	if err != nil {
		return 0, err
	}
	return int(C.call_has_persistent(f, h.j)), nil
}

func (h *ffiJournal) hasRuntimeFiles() (int, error) {
	f, err := ffiSymbol("sd_journal_has_runtime_files")
	if err != nil {
		return 0, err
	}
	return int(C.call_has_runtime(f, h.j)), nil
}

// batchAPINames is the resolved function-pointer table for the batch read
// loop, initialised once.
var batchAPINames = [...]string{
	"sd_journal_next",
	"sd_journal_previous",
	"sd_journal_restart_data",
	"sd_journal_enumerate_data",
	"sd_journal_get_realtime_usec",
	"sd_journal_get_cursor",
	"sd_journal_seek_cursor",
}

var (
	batchAPIOnce sync.Once
	batchAPIPtr  unsafe.Pointer
	batchAPIErr  error
)

// resolvedBatchAPI resolves the batch-loop symbols once and fills the C
// side table, deleting the per-call ffiSymbol map+mutex lookup the read
// loop used to pay for every field. The table lives in C memory so that
// passing it to the C loop never hands cgo a Go pointer to Go pointers.
func resolvedBatchAPI() (*C.jp_api, error) {
	batchAPIOnce.Do(func() {
		var ptrs [7]unsafe.Pointer
		for i, name := range batchAPINames {
			p, err := ffiSymbol(name)
			if err != nil {
				batchAPIErr = err
				return
			}
			ptrs[i] = p
		}
		ptr := C.malloc(C.size_t(unsafe.Sizeof(C.jp_api{})))
		if ptr == nil {
			batchAPIErr = errors.New("cannot allocate the batch API table")
			return
		}
		C.jp_api_init((*C.jp_api)(ptr), ptrs[0], ptrs[1], ptrs[2], ptrs[3], ptrs[4], ptrs[5], ptrs[6])
		batchAPIPtr = ptr
	})
	if batchAPIErr != nil {
		return nil, batchAPIErr
	}
	return (*C.jp_api)(batchAPIPtr), nil
}

// readBatchRaw runs one jp_read_batch call, returning the entry count, the
// bytes written and the status (0 buffer full / max reached, 1 end of
// journal, <0 -errno).
func (h *ffiJournal) readBatchRaw(buf []byte, limit int) (count, used, status int, err error) {
	api, err := resolvedBatchAPI()
	if err != nil {
		return 0, 0, 0, err
	}
	var cused C.size_t
	var cstatus C.int
	ptr := (*C.uint8_t)(unsafe.Pointer(&buf[0])) //nolint:gocritic // false positive on cgo-generated pointer check
	count = int(C.jp_read_batch(api, h.j, ptr, C.size_t(len(buf)), C.int(limit), &cused, &cstatus))
	return count, int(cused), int(cstatus), nil
}

func (h *ffiJournal) close() {
	if h.j == nil {
		return
	}
	if f, err := ffiSymbol("sd_journal_close"); err == nil {
		C.call_close(f, h.j)
	}
	h.j = nil
}
