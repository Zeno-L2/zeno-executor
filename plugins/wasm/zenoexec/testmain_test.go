package zenoexec

import (
	"os"
	"strconv"
	"testing"
)

// TestMain lets the test binary act as the session child that serve
// starts: "__child <network> <gas budget>" runs the command loop, and
// "__crash" exits at once.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "__child":
			budget, _ := strconv.ParseUint(os.Args[3], 10, 64)
			if err := RunWithBudget(os.Args[2], budget, os.Stdin, os.Stdout); err != nil {
				os.Exit(1)
			}
			os.Exit(0)
		case "__crash":
			os.Exit(3)
		}
	}
	os.Exit(m.Run())
}

// childCommand is the argv of a session child that the test binary
// provides.
func childCommand() []string {
	return []string{os.Args[0], "__child", "testnet", strconv.Itoa(ServeGasBudget)}
}
