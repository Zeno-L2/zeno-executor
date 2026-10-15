package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const mod = "github.com/digitalSloth/go-executor/"

func TestImportDenyList(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", mod+"cmd/gateway").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	denied := map[string]bool{"github.com/zenon-network/go-zenon/wallet": true}
	for _, p := range []string{"internal/supervisor", "internal/ipc", "internal/batch", "runtime", "worker", "plugins/wasm", "plugins/wasm/internal/engine", "plugins/wasm/internal/module"} {
		denied[mod+p] = true
	}
	const wazero = "github.com/tetratelabs/wazero"
	saw := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		saw[line] = true
		if denied[line] || line == wazero || strings.HasPrefix(line, wazero+"/") {
			t.Errorf("cmd/gateway depends on %s", line)
		}
	}
	for _, p := range []string{"internal/gateway", "plugins/wasm/reader", "internal/simipc", "internal/workerproc"} {
		if !saw[mod+p] {
			t.Fatalf("go list -deps did not list %s:\n%s", p, out)
		}
	}
}

// pkgNode is one package of a go list -deps graph.
type pkgNode struct {
	dir     string
	imports []string
}

// graph maps an import path to its package.
type graph map[string]pkgNode

const (
	gatewayPkg    = mod + "cmd/gateway"
	workerprocPkg = mod + "internal/workerproc"
)

// execBaseline and sysBaseline hold, per GOOS, the packages of the
// gateway's dependency closure that import os/exec and
// golang.org/x/sys/... today, each with why. The closure check keys on
// the importing package, never the full import path, and fails on a
// listed package the closure no longer holds.
var execBaseline = map[string]map[string]string{
	"linux":  {gopsutilCommon: gopsutilReason},
	"darwin": {gopsutilCommon: gopsutilReason},
}

const (
	gopsutilCommon = "github.com/shirou/gopsutil/internal/common"
	gopsutilReason = "reached via go-ethereum/metrics through go-zenon/common; the gateway calls no gopsutil API"
	metricsReason  = "reached through go-zenon/common; the gateway calls no metrics API"
	sysconfReason  = "reached via go-ethereum/metrics through go-zenon/common; the gateway calls no go-sysconf API"
	isattyReason   = "a terminal check in a logging dependency of go-ethereum/metrics"
	sha3Reason     = "CPU feature detection"
	sysUnix        = "golang.org/x/sys/unix"
	sysCPU         = "golang.org/x/sys/cpu"
)

// sysImport is one import of a golang.org/x/sys package by a package
// of the closure.
type sysImport struct{ importer, sys string }

// sysBaseline holds, per GOOS, the exact golang.org/x/sys package each
// importer may import, so an importer that moves to another x/sys
// package fails the closure check.
var sysBaseline = map[string]map[sysImport]string{
	"linux": {
		{"github.com/ethereum/go-ethereum/metrics", sysUnix}: metricsReason,
		{"github.com/mattn/go-isatty", sysUnix}:              isattyReason,
		{"github.com/tklauser/go-sysconf", sysUnix}:          sysconfReason,
		{"github.com/tklauser/numcpus", sysUnix}:             "reached via go-sysconf; the gateway calls no numcpus API",
		{"golang.org/x/crypto/sha3", sysCPU}:                 sha3Reason,
	},
	"darwin": {
		{"github.com/ethereum/go-ethereum/metrics", sysUnix}: metricsReason,
		{"github.com/mattn/go-isatty", sysUnix}:              isattyReason,
		{"github.com/shirou/gopsutil/cpu", sysUnix}:          gopsutilReason,
		{gopsutilCommon, sysUnix}:                            gopsutilReason,
		{"github.com/tklauser/go-sysconf", sysUnix}:          sysconfReason,
		{"golang.org/x/crypto/sha3", sysCPU}:                 sha3Reason,
	},
}

// listGraph returns the go list -deps graph of cmd/gateway for goos
// with cgo off.
func listGraph(t *testing.T, goos string) graph {
	t.Helper()
	cmd := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}\t{{.Dir}}\t{{join .Imports \",\"}}", gatewayPkg)
	cmd.Env = append(os.Environ(), "GOOS="+goos, "CGO_ENABLED=0")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("GOOS=%s go list -deps: %v\n%s", goos, err, out)
	}
	g := graph{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Split(line, "\t")
		if len(f) != 3 {
			t.Fatalf("GOOS=%s go list: unexpected line %q", goos, line)
		}
		var imports []string
		if f[2] != "" {
			imports = strings.Split(f[2], ",")
		}
		g[f[0]] = pkgNode{dir: f[1], imports: imports}
	}
	return g
}

// walk returns the packages reachable from the gateway, each with the
// import chain that reaches it. It does not enter internal/workerproc
// unless enterWorkerproc is set.
func (g graph) walk(enterWorkerproc bool) map[string][]string {
	chains := map[string][]string{gatewayPkg: {gatewayPkg}}
	queue := []string{gatewayPkg}
	for len(queue) > 0 {
		p := queue[0]
		queue = queue[1:]
		for _, i := range g[p].imports {
			if _, seen := chains[i]; seen {
				continue
			}
			if i == workerprocPkg && !enterWorkerproc {
				continue
			}
			chains[i] = append(slices.Clone(chains[p]), i)
			queue = append(queue, i)
		}
	}
	return chains
}

func importsPath(n pkgNode, want func(string) bool) bool {
	return slices.ContainsFunc(n.imports, want)
}

func isOSExec(p string) bool      { return p == "os/exec" }
func isXSys(p string) bool        { return strings.HasPrefix(p, "golang.org/x/sys/") }
func chainText(c []string) string { return strings.Join(c, "\n\t-> ") }

// checkGraph applies the reachability rule to g for goos: a package
// reachable from cmd/gateway without entering internal/workerproc may
// import os/exec or golang.org/x/sys/... only when the baseline lists
// it, and every baseline entry must still hold.
func checkGraph(goos string, g graph) []string {
	var out []string
	reach := g.walk(false)
	paths := make([]string, 0, len(reach))
	for p := range reach {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		n := g[p]
		if importsPath(n, isOSExec) && execBaseline[goos][p] == "" {
			out = append(out, fmt.Sprintf("GOOS=%s: %s imports os/exec and is not in execBaseline:\n\t%s\n\t-> os/exec", goos, p, chainText(reach[p])))
		}
		for _, i := range n.imports {
			if isXSys(i) && sysBaseline[goos][sysImport{p, i}] == "" {
				out = append(out, fmt.Sprintf("GOOS=%s: %s imports %s and is not in sysBaseline:\n\t%s\n\t-> %s", goos, p, i, chainText(reach[p]), i))
			}
		}
	}
	stale := func(table map[string]string, name string, imports func(string) bool) {
		for p := range table {
			if _, ok := reach[p]; !ok || !importsPath(g[p], imports) {
				out = append(out, fmt.Sprintf("GOOS=%s: %s is in %s but the closure no longer holds it as an importer; remove the entry", goos, p, name))
			}
		}
	}
	stale(execBaseline[goos], "execBaseline", isOSExec)
	for e := range sysBaseline[goos] {
		if _, ok := reach[e.importer]; !ok || !slices.Contains(g[e.importer].imports, e.sys) {
			out = append(out, fmt.Sprintf("GOOS=%s: %s is in sysBaseline for %s but the closure no longer holds that import; remove the entry", goos, e.importer, e.sys))
		}
	}
	sort.Strings(out)
	return out
}

// moduleRel returns p relative to the module, and whether p is a module
// package.
func moduleRel(p string) (string, bool) {
	if p == strings.TrimSuffix(mod, "/") {
		return "", true
	}
	return strings.CutPrefix(p, mod)
}

// importName returns the name a file gives an import: its alias, else
// the last element of its path.
func importName(spec *ast.ImportSpec, p string) string {
	if spec.Name != nil {
		return spec.Name.Name
	}
	return path.Base(p)
}

// scanFile reports each process-start rule that src, the file named
// file of the module package pkg (its import path relative to the
// module), breaks. internal/workerproc is exempt.
func scanFile(pkg, file string, src []byte) []string {
	if pkg == "internal/workerproc" {
		return nil
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, src, parser.ParseComments)
	if err != nil {
		return []string{fmt.Sprintf("%s/%s: does not parse: %v", pkg, file, err)}
	}
	var out []string
	report := func(pos token.Pos, format string, args ...any) {
		out = append(out, fmt.Sprintf("%s/%s:%d: %s", pkg, file, fset.Position(pos).Line, fmt.Sprintf(format, args...)))
	}
	names := map[string]string{} // local name -> import path
	for _, spec := range f.Imports {
		p, _ := strconv.Unquote(spec.Path.Value)
		switch {
		case spec.Name != nil && spec.Name.Name == ".":
			report(spec.Pos(), "dot import of %s", p)
			continue
		case p == "os/exec", p == "C", strings.HasPrefix(p, "golang.org/x/sys/"):
			report(spec.Pos(), "imports %s", p)
		case p == mod+"internal/workerproc" && !(pkg == "internal/gateway" && file == "simulate.go"):
			report(spec.Pos(), "imports internal/workerproc outside internal/gateway/simulate.go")
		}
		if spec.Name == nil || (spec.Name.Name != "_" && spec.Name.Name != ".") {
			names[importName(spec, p)] = p
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok {
			return true
		}
		name := sel.Sel.Name
		switch names[id.Name] {
		case "os":
			if name == "StartProcess" {
				report(sel.Pos(), "os.StartProcess")
			}
		case "syscall":
			if name == "ForkExec" || name == "Exec" || name == "StartProcess" ||
				strings.HasPrefix(name, "Syscall") || strings.HasPrefix(name, "RawSyscall") || strings.HasPrefix(name, "AllThreadsSyscall") {
				report(sel.Pos(), "syscall.%s", name)
			}
		case mod + "internal/simipc":
			if name == "ReadFromGateway" {
				report(sel.Pos(), "simipc.ReadFromGateway: a gateway reads simulator frames with ReadFromSimulator")
			}
		}
		return true
	})
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			if strings.HasPrefix(c.Text, "//go:linkname") {
				report(c.Pos(), "//go:linkname")
			}
		}
	}
	return out
}

func TestOnlySimulatorsStartProcesses(t *testing.T) {
	graphs := map[string]graph{}
	for _, goos := range []string{"linux", "darwin"} {
		g := listGraph(t, goos)
		graphs[goos] = g
		for _, f := range checkGraph(goos, g) {
			t.Error(f)
		}
		// The rule is live: os/exec is reached through the baseline
		// and, once workerproc may be entered, through workerproc.
		viaWorkerproc := false
		for p := range g.walk(true) {
			if importsPath(g[p], isOSExec) && p == workerprocPkg {
				viaWorkerproc = true
			}
		}
		if !viaWorkerproc {
			t.Errorf("GOOS=%s: os/exec is not reached through internal/workerproc", goos)
		}
		if !importsPath(g[gopsutilCommon], isOSExec) {
			t.Errorf("GOOS=%s: %s does not import os/exec", goos, gopsutilCommon)
		}
	}

	// Every module package of either closure, scanned file by file
	// whatever the file's build tags.
	dirs := map[string]string{}
	for _, g := range graphs {
		for p, n := range g {
			if rel, ok := moduleRel(p); ok && p != workerprocPkg {
				dirs[rel] = n.dir
			}
		}
	}
	for _, need := range []string{"internal/gateway", "internal/simipc"} {
		if _, ok := dirs[need]; !ok {
			t.Fatalf("the closure holds no %s", need)
		}
	}
	if _, ok := graphs["linux"][workerprocPkg]; !ok {
		t.Fatal("the closure holds no internal/workerproc")
	}
	rels := make([]string, 0, len(dirs))
	for rel := range dirs {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	for _, rel := range rels {
		files, err := filepath.Glob(filepath.Join(dirs[rel], "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			src, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range scanFile(rel, filepath.Base(f), src) {
				t.Error(r)
			}
		}
	}

	// simulate.go is where the gateway starts and reads simulators.
	src, err := os.ReadFile(filepath.Join(dirs["internal/gateway"], "simulate.go"))
	if err != nil {
		t.Fatalf("internal/gateway/simulate.go: %v", err)
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "simulate.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	importsWorkerproc := false
	for _, spec := range f.Imports {
		if p, _ := strconv.Unquote(spec.Path.Value); p == workerprocPkg {
			importsWorkerproc = true
		}
	}
	readsSimulator := false
	ast.Inspect(f, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok {
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == "simipc" && sel.Sel.Name == "ReadFromSimulator" {
				readsSimulator = true
			}
		}
		return true
	})
	if !importsWorkerproc || !readsSimulator {
		t.Errorf("internal/gateway/simulate.go: imports internal/workerproc %v, names simipc.ReadFromSimulator %v; want both", importsWorkerproc, readsSimulator)
	}
}

func TestProcessScanCatches(t *testing.T) {
	const head = "package p\n"
	scans := []struct {
		name, pkg, file, src, want string
	}{
		{"dot import", "internal/gateway", "a.go", head + "import . \"os\"\n", "dot import"},
		{"uncalled ForkExec", "internal/gateway", "a.go", head + "import \"syscall\"\nvar f = syscall.ForkExec\n", "syscall.ForkExec"},
		{"aliased os/exec", "internal/gateway", "a.go", head + "import x \"os/exec\"\nvar _ = x.Command\n", "imports os/exec"},
		{"x/sys", "internal/gateway", "a.go", head + "import \"golang.org/x/sys/unix\"\n", "imports golang.org/x/sys/unix"},
		{"cgo", "internal/gateway", "a.go", head + "import \"C\"\n", "imports C"},
		{"RawSyscall", "internal/gateway", "a.go", head + "import \"syscall\"\nfunc f() { syscall.RawSyscall(0, 0, 0, 0) }\n", "syscall.RawSyscall"},
		{"ReadFromGateway", "internal/gateway", "a.go", head + "import \"" + mod + "internal/simipc\"\nvar f = simipc.ReadFromGateway\n", "simipc.ReadFromGateway"},
		{"workerproc import", "internal/gateway", "other.go", head + "import _ \"" + mod + "internal/workerproc\"\n", "imports internal/workerproc outside"},
		{"AllThreadsSyscall", "internal/gateway", "a.go", head + "import \"syscall\"\nfunc f() { syscall.AllThreadsSyscall(0, 0, 0, 0) }\n", "syscall.AllThreadsSyscall"},
		{"SyscallNoError", "internal/gateway", "a.go", head + "import \"syscall\"\nfunc f() { syscall.SyscallNoError(0, 0, 0, 0) }\n", "syscall.SyscallNoError"},
		{"os.StartProcess", "internal/gateway", "a.go", head + "import \"os\"\nvar f = os.StartProcess\n", "os.StartProcess"},
		{"aliased syscall", "internal/gateway", "a.go", head + "import sc \"syscall\"\nvar f = sc.Exec\n", "syscall.Exec"},
		{"linkname", "internal/gateway", "a.go", head + "//go:linkname f runtime.f\nfunc f()\n", "//go:linkname"},
	}
	for _, c := range scans {
		t.Run(c.name, func(t *testing.T) {
			got := scanFile(c.pkg, c.file, []byte(c.src))
			if len(got) == 0 || !strings.Contains(strings.Join(got, "\n"), c.want) {
				t.Fatalf("scanFile reports %q, want a report containing %q", got, c.want)
			}
		})
	}
	allowed := []struct{ name, pkg, file, src string }{
		{"simulate.go starts simulators", "internal/gateway", "simulate.go", head + "import (\n\t\"syscall\"\n\t\"" + mod + "internal/workerproc\"\n)\nvar _ = workerproc.NewHost\nfunc f() { syscall.Kill(1, 9) }\n"},
		{"workerproc runs processes", "internal/workerproc", "workerproc.go", head + "import \"os/exec\"\nimport \"syscall\"\nvar _ = exec.Command\nvar _ = syscall.ForkExec\n"},
		{"another package's own simipc name", "internal/gateway", "a.go", head + "import \"fmt\"\nvar simipc = fmt.Sprint\nvar _ = simipc\n"},
		{"ReadFromSimulator", "internal/gateway", "a.go", head + "import \"" + mod + "internal/simipc\"\nvar f = simipc.ReadFromSimulator\n"},
	}
	for _, c := range allowed {
		t.Run(c.name, func(t *testing.T) {
			if got := scanFile(c.pkg, c.file, []byte(c.src)); len(got) != 0 {
				t.Fatalf("scanFile reports %q, want nothing", got)
			}
		})
	}

	// In-memory graphs.
	base := func(goos string) graph {
		g := graph{gatewayPkg: {}}
		node := func(p string) pkgNode { return g[p] }
		add := func(p, imp string) {
			n := node(p)
			n.imports = append(n.imports, imp)
			g[p] = n
		}
		for p := range execBaseline[goos] {
			add(gatewayPkg, p)
			add(p, "os/exec")
		}
		for e := range sysBaseline[goos] {
			add(gatewayPkg, e.importer)
			add(e.importer, e.sys)
		}
		return g
	}
	add := func(g graph, p, imp string) {
		n := g[p]
		n.imports = append(n.imports, imp)
		g[p] = n
	}
	for _, goos := range []string{"linux", "darwin"} {
		t.Run("graph "+goos, func(t *testing.T) {
			if got := checkGraph(goos, base(goos)); len(got) != 0 {
				t.Fatalf("the baseline graph reports %q", got)
			}
			g := base(goos)
			add(g, gatewayPkg, "net/http/cgi")
			add(g, "net/http/cgi", "os/exec")
			if got := checkGraph(goos, g); len(got) != 1 || !strings.Contains(got[0], gatewayPkg+"\n\t-> net/http/cgi\n\t-> os/exec") {
				t.Fatalf("net/http/cgi: %q", got)
			}
			g = base(goos)
			add(g, gatewayPkg, mod+"internal/gateway")
			add(g, mod+"internal/gateway", "example.com/third")
			add(g, "example.com/third", "os/exec")
			if got := checkGraph(goos, g); len(got) != 1 || !strings.Contains(got[0], "example.com/third imports os/exec") ||
				!strings.Contains(got[0], gatewayPkg+"\n\t-> "+mod+"internal/gateway\n\t-> example.com/third") {
				t.Fatalf("a non-baseline os/exec importer: %q", got)
			}
			g = base(goos)
			add(g, gatewayPkg, "example.com/other")
			add(g, "example.com/other", "golang.org/x/sys/unix")
			if got := checkGraph(goos, g); len(got) != 1 || !strings.Contains(got[0], "example.com/other imports golang.org/x/sys") {
				t.Fatalf("a non-baseline x/sys importer: %q", got)
			}
			g = base(goos)
			moved := sysImport{"golang.org/x/crypto/sha3", sysCPU}
			if _, ok := sysBaseline[goos][moved]; !ok {
				t.Fatalf("sysBaseline[%s] lacks %v", goos, moved)
			}
			g["golang.org/x/crypto/sha3"] = pkgNode{imports: []string{sysUnix}}
			got := checkGraph(goos, g)
			if len(got) != 2 || !strings.Contains(strings.Join(got, "\n"), "golang.org/x/crypto/sha3 imports "+sysUnix+" and is not in sysBaseline") ||
				!strings.Contains(strings.Join(got, "\n"), "golang.org/x/crypto/sha3 is in sysBaseline for "+sysCPU) {
				t.Fatalf("an importer that moves from x/sys/cpu to x/sys/unix: %q", got)
			}
			g = base(goos)
			add(g, gatewayPkg, "golang.org/x/sys/execabs")
			add(g, "golang.org/x/sys/execabs", "os/exec")
			if got := checkGraph(goos, g); !strings.Contains(strings.Join(got, "\n"), "golang.org/x/sys/execabs imports os/exec") {
				t.Fatalf("x/sys/execabs: %q", got)
			}
			g = base(goos)
			add(g, gatewayPkg, workerprocPkg)
			add(g, workerprocPkg, "os/exec")
			add(g, workerprocPkg, "golang.org/x/sys/unix")
			if got := checkGraph(goos, g); len(got) != 0 {
				t.Fatalf("os/exec reached only through internal/workerproc: %q", got)
			}
			if !importsPath(g[workerprocPkg], isOSExec) || len(g.walk(true)) <= len(g.walk(false)) {
				t.Fatalf("the walk does not enter internal/workerproc when allowed")
			}
			for p := range execBaseline[goos] {
				g = base(goos)
				delete(g, p)
				g[gatewayPkg] = pkgNode{imports: slices.DeleteFunc(slices.Clone(g[gatewayPkg].imports), func(i string) bool { return i == p })}
				got := checkGraph(goos, g)
				if len(got) == 0 || !strings.Contains(strings.Join(got, "\n"), p+" is in execBaseline") {
					t.Fatalf("a baseline entry the graph lacks (%s): %q", p, got)
				}
				g = base(goos)
				g[p] = pkgNode{}
				got = checkGraph(goos, g)
				if len(got) == 0 || !strings.Contains(strings.Join(got, "\n"), p+" is in execBaseline") {
					t.Fatalf("a baseline entry that no longer imports os/exec (%s): %q", p, got)
				}
			}
		})
	}
}
