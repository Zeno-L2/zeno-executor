package ipc

// Worker exit statuses. 0 is a clean shutdown. Every other status, and death by any signal,
// is a failure.
const (
	ExitHalted   = 3  // the worker halted on a consistency fault; never restarted
	ExitTempFail = 75 // sysexits EX_TEMPFAIL: source unavailable or domain lock held; restarted, never counted toward the crash loop
	ExitConfig   = 78 // sysexits EX_CONFIG: the worker rejected its configuration; never restarted
)
