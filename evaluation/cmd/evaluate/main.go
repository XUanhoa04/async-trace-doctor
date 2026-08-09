package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/XUanhoa04/async-trace-doctor/internal/config"
	"github.com/XUanhoa04/async-trace-doctor/internal/ingest"
	"github.com/XUanhoa04/async-trace-doctor/internal/model"
	"github.com/XUanhoa04/async-trace-doctor/internal/rules"
	"gopkg.in/yaml.v3"
)

type manifest struct {
	Dataset string  `yaml:"dataset"`
	Cases   []truth `yaml:"cases"`
}
type truth struct {
	Name, Input        string
	Normal             bool           `yaml:"normal"`
	Broken             bool           `yaml:"brokenLink"`
	ExpectedRules      []string       `yaml:"expectedRules"`
	ExpectedRuleCounts map[string]int `yaml:"expectedRuleCounts"`
	ExpectedEdges      []model.Edge   `yaml:"expectedEdges"`
}
type scores struct {
	Cases                   int                `json:"cases"`
	BrokenPrecision         float64            `json:"broken_link_precision"`
	BrokenRecall            float64            `json:"broken_link_recall"`
	BrokenF1                float64            `json:"broken_link_f1"`
	ViolationRecall         map[string]float64 `json:"violation_recall_by_rule"`
	ViolationPrecision      map[string]float64 `json:"violation_precision_by_rule"`
	TopologyEdgeAccuracy    float64            `json:"topology_edge_accuracy"`
	NormalFalsePositiveRate float64            `json:"normal_false_positive_rate"`
	ExactCaseAccuracy       float64            `json:"exact_finding_set_accuracy"`
	NonExactRuleCases       int                `json:"non_exact_finding_cases"`
}
type output struct {
	SchemaVersion           string     `json:"schema_version"`
	GeneratedAt             time.Time  `json:"generated_at"`
	RulesSemanticConvention string     `json:"rules_semantic_convention"`
	UnitGolden              scores     `json:"unit_golden"`
	Holdout                 scores     `json:"holdout"`
	Adversarial             scores     `json:"adversarial"`
	LiveDocker              any        `json:"live_docker"`
	Notes                   []string   `json:"notes"`
	Provenance              provenance `json:"provenance"`
}

type provenance struct {
	GitCommit        string            `json:"git_commit"`
	GitWorktreeDirty bool              `json:"git_worktree_dirty"`
	GoVersion        string            `json:"go_version"`
	SourceSHA256     string            `json:"source_sha256"`
	RulesSHA256      string            `json:"rules_sha256"`
	DatasetSHA256    map[string]string `json:"dataset_sha256"`
}

func main() {
	rulesPath := flag.String("rules", "config/rules.yaml", "rules config")
	outPath := flag.String("output", "evaluation/results/latest.json", "result path")
	flag.Parse()
	cfg, err := config.Load(*rulesPath)
	must(err)
	core, coreHash, err := evaluate("evaluation/datasets/core/ground_truth.yaml", cfg)
	must(err)
	holdout, holdoutHash, err := evaluate("evaluation/datasets/holdout/ground_truth.yaml", cfg)
	must(err)
	adversarial, adversarialHash, err := evaluate("evaluation/datasets/adversarial/ground_truth.yaml", cfg)
	must(err)
	live := any(map[string]any{"status": "not_run", "scenarios": []any{}})
	currentCommit := gitCommit()
	currentRulesHash := fileHash(*rulesPath)
	if b, err := os.ReadFile("evaluation/results/live.json"); err == nil {
		var v any
		if json.Unmarshal(b, &v) == nil {
			live = validateLiveProvenance(v, currentCommit, currentRulesHash)
		}
	}
	o := output{SchemaVersion: "2.0", GeneratedAt: time.Now().UTC(), RulesSemanticConvention: cfg.SemanticConventionVersion, UnitGolden: core, Holdout: holdout, Adversarial: adversarial, LiveDocker: live, Notes: []string{"Ground truth was loaded by the evaluator only after each audit input was processed.", "Scores describe the bundled synthetic cases only; they are not production accuracy claims.", "Live results are marked stale when commit or rules provenance does not match.", "Performance is reported only by the separate reproducible correlation benchmarks."}, Provenance: provenance{GitCommit: currentCommit, GitWorktreeDirty: gitWorktreeDirty(), GoVersion: runtime.Version(), SourceSHA256: sourceHash(), RulesSHA256: currentRulesHash, DatasetSHA256: map[string]string{"core": coreHash, "holdout": holdoutHash, "adversarial": adversarialHash}}}
	must(os.MkdirAll(filepath.Dir(*outPath), 0755))
	f, err := os.Create(*outPath)
	must(err)
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	must(enc.Encode(o))
	must(f.Close())
	fmt.Printf("wrote %s\n", *outPath)
}
func evaluate(path string, cfg config.Config) (scores, string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return scores{}, "", err
	}
	var m manifest
	if err = yaml.Unmarshal(b, &m); err != nil {
		return scores{}, "", err
	}
	base := filepath.Dir(path)
	s := scores{Cases: len(m.Cases), ViolationRecall: map[string]float64{}, ViolationPrecision: map[string]float64{}}
	tp, fp, fn := 0, 0, 0
	expectedByRule := map[string]int{}
	foundByRule := map[string]int{}
	predictedByRule := map[string]int{}
	edgeExpected, edgeObserved, edgeFound := 0, 0, 0
	normal, totalNormal := 0, 0
	exactCases := 0
	hashPaths := []string{path}
	for _, tc := range m.Cases {
		input := filepath.Clean(filepath.Join(base, tc.Input))
		hashPaths = append(hashPaths, input)
		spans, err := ingest.ReadPath(input, ingest.Limits{MaxBytes: 64 << 20, MaxSpans: 100000}, cfg.RedactAttributes)
		if err != nil {
			return s, "", fmt.Errorf("%s: %w", tc.Name, err)
		}
		r := rules.Engine{Config: cfg}.Audit(spans)
		found := map[string]bool{}
		foundCounts := map[string]int{}
		for _, f := range r.Findings {
			found[f.RuleID] = true
			foundCounts[f.RuleID]++
		}
		expected := map[string]bool{}
		for _, id := range tc.ExpectedRules {
			expected[id] = true
		}
		exact := len(found) == len(expected)
		for id := range found {
			predictedByRule[id]++
			if !expected[id] {
				exact = false
			}
		}
		if tc.ExpectedRuleCounts != nil {
			if len(foundCounts) != len(tc.ExpectedRuleCounts) {
				exact = false
			}
			for id, count := range tc.ExpectedRuleCounts {
				if foundCounts[id] != count {
					exact = false
				}
			}
		}
		if exact {
			exactCases++
		} else {
			s.NonExactRuleCases++
		}
		pred := found["ATD-CTX-001"]
		if pred && tc.Broken {
			tp++
		} else if pred {
			fp++
		} else if tc.Broken {
			fn++
		}
		if tc.Normal {
			totalNormal++
			if len(r.Findings) > 0 {
				normal++
			}
		}
		for _, id := range tc.ExpectedRules {
			expectedByRule[id]++
			if found[id] {
				foundByRule[id]++
			}
		}
		edgeExpected += len(tc.ExpectedEdges)
		edgeObserved += len(r.Topology)
		edgeFound += matchedEdges(tc.ExpectedEdges, r.Topology)
	}
	s.BrokenPrecision = ratio(tp, tp+fp)
	s.BrokenRecall = ratio(tp, tp+fn)
	s.BrokenF1 = f1(s.BrokenPrecision, s.BrokenRecall)
	for id, n := range expectedByRule {
		s.ViolationRecall[id] = ratio(foundByRule[id], n)
	}
	allRules := map[string]bool{}
	for id := range expectedByRule {
		allRules[id] = true
	}
	for id := range predictedByRule {
		allRules[id] = true
	}
	for id := range allRules {
		s.ViolationPrecision[id] = ratio(foundByRule[id], predictedByRule[id])
	}
	s.TopologyEdgeAccuracy = ratio(edgeFound, edgeExpected+edgeObserved-edgeFound)
	if edgeExpected == 0 && edgeObserved == 0 {
		s.TopologyEdgeAccuracy = 1
	}
	s.NormalFalsePositiveRate = ratio(normal, totalNormal)
	s.ExactCaseAccuracy = ratio(exactCases, s.Cases)
	return s, hashFiles(hashPaths), nil
}
func matchedEdges(expected, observed []model.Edge) int {
	matched := 0
	for _, x := range expected {
		for _, o := range observed {
			if x.Producer == o.Producer && x.Consumer == o.Consumer && x.System == o.System && x.Destination == o.Destination &&
				(x.ConsumerGroup == "" || x.ConsumerGroup == o.ConsumerGroup) &&
				(x.Subscription == "" || x.Subscription == o.Subscription) &&
				(x.Environment == "" || x.Environment == o.Environment) &&
				(x.ServiceNamespace == "" || x.ServiceNamespace == o.ServiceNamespace) &&
				(x.DestinationNamespace == "" || x.DestinationNamespace == o.DestinationNamespace) &&
				(x.BrokerAddress == "" || x.BrokerAddress == o.BrokerAddress) &&
				(x.Count == 0 || x.Count == o.Count) {
				matched++
				break
			}
		}
	}
	return matched
}
func ratio(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}
func f1(p, r float64) float64 {
	if p+r == 0 {
		return 0
	}
	return 2 * p * r / (p + r)
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}

// fileHash hashes only file contents so producers written in other languages can
// reproduce the value without knowing the evaluator's path-framing convention.
func fileHash(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", sha256.Sum256(b))
}

func hashFiles(paths []string) string {
	sort.Strings(paths)
	h := sha256.New()
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		_, _ = h.Write([]byte(filepath.ToSlash(path)))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(b)
		_, _ = h.Write([]byte{0})
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func gitCommit() string {
	if value := os.Getenv("GITHUB_SHA"); value != "" {
		return value
	}
	cmd := exec.Command("git", "rev-parse", "HEAD")
	b, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(b))
}

func gitWorktreeDirty() bool {
	cmd := exec.Command("git", "status", "--porcelain")
	b, err := cmd.Output()
	return err != nil || strings.TrimSpace(string(b)) != ""
}

func sourceHash() string {
	paths := []string{"go.mod", "go.sum"}
	for _, root := range []string{"cmd", "internal", "evaluation/cmd", "config"} {
		_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info == nil || info.IsDir() {
				return nil
			}
			extension := strings.ToLower(filepath.Ext(path))
			if extension == ".go" || extension == ".yaml" || extension == ".yml" {
				paths = append(paths, path)
			}
			return nil
		})
	}
	return hashFiles(paths)
}

func validateLiveProvenance(value any, commit, rulesHash string) any {
	artifact, ok := value.(map[string]any)
	if !ok {
		return map[string]any{"status": "stale", "provenance_status": "invalid", "scenarios": []any{}}
	}
	artifactCommit, _ := artifact["git_commit"].(string)
	artifactRulesHash, _ := artifact["rules_sha256"].(string)
	artifactDirty, hasDirtyState := artifact["git_worktree_dirty"].(bool)
	if artifactCommit != commit || artifactRulesHash != rulesHash || !hasDirtyState || artifactDirty {
		artifact["status"] = "stale"
		artifact["provenance_status"] = "stale"
		return artifact
	}
	artifact["provenance_status"] = "current"
	return artifact
}
