package chdbpurego

import (
	"os"
	"os/exec"
	"unsafe"

	"github.com/ebitengine/purego"
)

func findLibrary() string {
	// Env var
	if envPath := os.Getenv("CHDB_LIB_PATH"); envPath != "" {
		return envPath
	}

	// ldconfig with Linux
	if path, err := exec.LookPath("libchdb.so"); err == nil {
		return path
	}

	// default path
	commonPaths := []string{
		"/usr/local/lib/libchdb.so",
		"/opt/homebrew/lib/libchdb.dylib",
	}

	for _, p := range commonPaths {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}

	//should be an error ?
	return "libchdb.so"
}

var (
	// old API
	queryStable            func(argc int, argv []string) *local_result
	freeResult             func(result *local_result)
	queryStableV2          func(argc int, argv []string) *local_result_v2
	freeResultV2           func(result *local_result_v2)
	connectChdb            func(argc int, argv []*byte) **chdb_conn
	closeConn              func(conn **chdb_conn)
	queryConn              func(conn *chdb_conn, query string, format string) *local_result_v2
	queryConnStreaming     func(conn *chdb_conn, query string, format string) *chdb_streaming_result
	streamingResultError   func(result *chdb_streaming_result) *string
	streamingResultNext    func(conn *chdb_conn, result *chdb_streaming_result) *local_result_v2
	streamingResultDestroy func(result *chdb_streaming_result)
	streamingResultCancel  func(conn *chdb_conn, result *chdb_streaming_result)

	// new API
	chdbConnect                func(argc int, argv []*byte) *chdb_connection
	chdbCloseConn              func(conn unsafe.Pointer)
	chdbQuery                  func(conn unsafe.Pointer, query string, format string) *chdb_result
	chdbStreamQuery            func(conn unsafe.Pointer, query string, format string) *chdb_result
	chdbStreamFetchResult      func(conn unsafe.Pointer, result *chdb_result) *chdb_result
	chdbStreamCancelQuery      func(conn unsafe.Pointer, result *chdb_result)
	chdbDestroyQueryResult     func(result *chdb_result)
	chdbResultBuffer           func(result *chdb_result) *byte
	chdbResultLen              func(result *chdb_result) uint    //size_t
	chdbResultElapsed          func(result *chdb_result) float64 // double
	chdbResultRowsRead         func(result *chdb_result) uint64
	chdbResultBytesRead        func(result *chdb_result) uint64
	chdbResultStorageRowsRead  func(result *chdb_result) uint64
	chdbResultStorageBytesRead func(result *chdb_result) uint64
	chdbResultError            func(result *chdb_result) string
)

func init() {
	path := findLibrary()
	libchdb, err := purego.Dlopen(path, purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		panic(err)
	}
	purego.RegisterLibFunc(&queryStable, libchdb, "query_stable")
	purego.RegisterLibFunc(&freeResult, libchdb, "free_result")
	purego.RegisterLibFunc(&queryStableV2, libchdb, "query_stable_v2")

	purego.RegisterLibFunc(&freeResultV2, libchdb, "free_result_v2")
	purego.RegisterLibFunc(&connectChdb, libchdb, "connect_chdb")
	purego.RegisterLibFunc(&closeConn, libchdb, "close_conn")
	purego.RegisterLibFunc(&queryConn, libchdb, "query_conn")
	purego.RegisterLibFunc(&queryConnStreaming, libchdb, "query_conn_streaming")
	purego.RegisterLibFunc(&streamingResultError, libchdb, "chdb_streaming_result_error")
	purego.RegisterLibFunc(&streamingResultNext, libchdb, "chdb_streaming_fetch_result")
	purego.RegisterLibFunc(&streamingResultCancel, libchdb, "chdb_streaming_cancel_query")
	purego.RegisterLibFunc(&streamingResultDestroy, libchdb, "chdb_destroy_result")

	// new API
	purego.RegisterLibFunc(&chdbConnect, libchdb, "chdb_connect")
	purego.RegisterLibFunc(&chdbCloseConn, libchdb, "chdb_close_conn")
	purego.RegisterLibFunc(&chdbQuery, libchdb, "chdb_query")
	purego.RegisterLibFunc(&chdbStreamQuery, libchdb, "chdb_stream_query")
	purego.RegisterLibFunc(&chdbStreamFetchResult, libchdb, "chdb_stream_fetch_result")
	purego.RegisterLibFunc(&chdbStreamCancelQuery, libchdb, "chdb_stream_cancel_query")
	purego.RegisterLibFunc(&chdbDestroyQueryResult, libchdb, "chdb_destroy_query_result")
	purego.RegisterLibFunc(&chdbResultBuffer, libchdb, "chdb_result_buffer")
	purego.RegisterLibFunc(&chdbResultLen, libchdb, "chdb_result_length")
	purego.RegisterLibFunc(&chdbResultElapsed, libchdb, "chdb_result_elapsed")
	purego.RegisterLibFunc(&chdbResultRowsRead, libchdb, "chdb_result_rows_read")
	purego.RegisterLibFunc(&chdbResultBytesRead, libchdb, "chdb_result_bytes_read")
	purego.RegisterLibFunc(&chdbResultStorageRowsRead, libchdb, "chdb_result_storage_rows_read")
	purego.RegisterLibFunc(&chdbResultStorageBytesRead, libchdb, "chdb_result_storage_bytes_read")
	purego.RegisterLibFunc(&chdbResultError, libchdb, "chdb_result_error")

}
