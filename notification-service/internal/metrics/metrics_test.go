package metrics

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get current working directory: %v", err)
	}

	for {
		candidate := filepath.Join(dir, "ticketing-common", "src", "main", "java", "com", "ticketing", "common", "messaging", "EventTypes.java")
		if _, err := os.Stat(candidate); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not locate repository root starting from %s", dir)
		}
		dir = parent
	}
}

func TestCanonicalEventTypes_MatchesJavaEventTypes(t *testing.T) {
	repoRoot := findRepoRoot(t)
	javaFile := filepath.Join(repoRoot, "ticketing-common", "src", "main", "java", "com", "ticketing", "common", "messaging", "EventTypes.java")

	f, err := os.Open(javaFile)
	if err != nil {
		t.Fatalf("failed to open EventTypes.java at %s: %v", javaFile, err)
	}
	defer f.Close()

	// Pattern matching: public static final String CONST_NAME = "event.type";
	re := regexp.MustCompile(`public\s+static\s+final\s+String\s+\w+\s*=\s*"([^"]+)";`)
	scanner := bufio.NewScanner(f)
	javaEvents := make(map[string]bool)

	for scanner.Scan() {
		line := scanner.Text()
		matches := re.FindStringSubmatch(line)
		if len(matches) == 2 {
			javaEvents[matches[1]] = true
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("error scanning EventTypes.java: %v", err)
	}

	if len(javaEvents) == 0 {
		t.Fatalf("no event types extracted from %s; check parsing regex", javaFile)
	}

	// 1. Every event in Java EventTypes must be in CanonicalEventTypes
	for ev := range javaEvents {
		if !CanonicalEventTypes[ev] {
			t.Errorf("event %q defined in EventTypes.java is missing from CanonicalEventTypes", ev)
		}
	}

	// 2. Every event in CanonicalEventTypes must be in Java EventTypes (exact set equality)
	for ev := range CanonicalEventTypes {
		if !javaEvents[ev] {
			t.Errorf("CanonicalEventTypes contains %q which is not defined in EventTypes.java", ev)
		}
	}

	if len(CanonicalEventTypes) != len(javaEvents) {
		t.Errorf("size mismatch: CanonicalEventTypes has %d entries, EventTypes.java has %d entries",
			len(CanonicalEventTypes), len(javaEvents))
	}
}

func TestCanonicalEventTypes_MatchesDocsArch04Events(t *testing.T) {
	repoRoot := findRepoRoot(t)
	docFile := filepath.Join(repoRoot, "docs", "arch", "04-events.md")

	f, err := os.Open(docFile)
	if err != nil {
		t.Fatalf("failed to open docs/arch/04-events.md at %s: %v", docFile, err)
	}
	defer f.Close()

	// Pattern matching: #### 4.2.X `event.type`
	reSection := regexp.MustCompile(`^####\s+4\.2\.\d+\s+` + "`" + `([^` + "`" + `]+)` + "`")
	scanner := bufio.NewScanner(f)
	docEvents := make(map[string]bool)

	for scanner.Scan() {
		line := scanner.Text()
		matches := reSection.FindStringSubmatch(line)
		if len(matches) == 2 {
			docEvents[matches[1]] = true
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("error scanning docs/arch/04-events.md: %v", err)
	}

	if len(docEvents) == 0 {
		t.Fatalf("no event types found in section headers of %s", docFile)
	}

	// Assert every event type found in 04-events.md section headers is in CanonicalEventTypes
	for ev := range docEvents {
		if !CanonicalEventTypes[ev] {
			t.Errorf("event %q found in docs/arch/04-events.md is missing from CanonicalEventTypes", ev)
		}
	}
}
