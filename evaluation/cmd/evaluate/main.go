package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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
	Name, Input   string
	Normal        bool         `yaml:"normal"`
	Broken        bool         `yaml:"brokenLink"`
	ExpectedRules []string     `yaml:"expectedRules"`
	ExpectedEdges []model.Edge `yaml:"expectedEdges"`
}
type scores struct {
	Cases                   int                `json:"cases"`
	BrokenPrecision         float64            `json:"broken_link_precision"`
	BrokenRecall            float64            `json:"broken_link_recall"`
	BrokenF1                float64            `json:"broken_link_f1"`
	ViolationRecall         map[string]float64 `json:"violation_recall_by_rule"`
	TopologyEdgeAccuracy    float64            `json:"topology_edge_accuracy"`
	NormalFalsePositiveRate float64            `json:"normal_false_positive_rate"`
	ProcessingMillis        float64            `json:"processing_latency_ms_per_case"`
	PeakAllocatedBytes      uint64             `json:"peak_allocated_bytes"`
}
type output struct {
	GeneratedAt             time.Time `json:"generated_at"`
	RulesSemanticConvention string    `json:"rules_semantic_convention"`
	UnitGolden              scores    `json:"unit_golden"`
	Holdout                 scores    `json:"holdout"`
	LiveDocker              any       `json:"live_docker"`
	Notes                   []string  `json:"notes"`
}

func main() {
	rulesPath := flag.String("rules", "config/rules.yaml", "rules config")
	outPath := flag.String("output", "evaluation/results/latest.json", "result path")
	flag.Parse()
	cfg, err := config.Load(*rulesPath)
	must(err)
	core, err := evaluate("evaluation/datasets/core/ground_truth.yaml", cfg)
	must(err)
	holdout, err := evaluate("evaluation/datasets/holdout/ground_truth.yaml", cfg)
	must(err)
	live := any(map[string]any{"status": "not_run", "scenarios": []any{}})
	if b, err := os.ReadFile("evaluation/results/live.json"); err == nil {
		var v any
		if json.Unmarshal(b, &v) == nil {
			live = v
		}
	}
	o := output{GeneratedAt: time.Now().UTC(), RulesSemanticConvention: cfg.SemanticConventionVersion, UnitGolden: core, Holdout: holdout, LiveDocker: live, Notes: []string{"Ground truth was loaded by the evaluator only after each audit input was processed.", "Peak allocated bytes is a process-level runtime sample, not a container RSS measurement."}}
	must(os.MkdirAll(filepath.Dir(*outPath), 0755))
	f, err := os.Create(*outPath)
	must(err)
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	must(enc.Encode(o))
	must(f.Close())
	fmt.Printf("wrote %s\n", *outPath)
}
func evaluate(path string, cfg config.Config) (scores, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return scores{}, err
	}
	var m manifest
	if err = yaml.Unmarshal(b, &m); err != nil {
		return scores{}, err
	}
	base := filepath.Dir(path)
	s := scores{Cases: len(m.Cases), ViolationRecall: map[string]float64{}}
	tp, fp, fn := 0, 0, 0
	expectedByRule := map[string]int{}
	foundByRule := map[string]int{}
	edgeExpected, edgeObserved, edgeFound := 0, 0, 0
	normal, totalNormal := 0, 0
	var peak uint64
	for _, tc := range m.Cases {
		input := filepath.Clean(filepath.Join(base, tc.Input))
		spans, err := ingest.ReadPath(input, ingest.Limits{MaxBytes: 64 << 20, MaxSpans: 100000}, cfg.RedactAttributes)
		if err != nil {
			return s, fmt.Errorf("%s: %w", tc.Name, err)
		}
		start := time.Now()
		r := rules.Engine{Config: cfg}.Audit(spans)
		s.ProcessingMillis += float64(time.Since(start).Nanoseconds()) / 1e6
		found := map[string]bool{}
		for _, f := range r.Findings {
			found[f.RuleID] = true
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
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		if ms.Alloc > peak {
			peak = ms.Alloc
		}
	}
	s.BrokenPrecision = ratio(tp, tp+fp)
	s.BrokenRecall = ratio(tp, tp+fn)
	s.BrokenF1 = f1(s.BrokenPrecision, s.BrokenRecall)
	for id, n := range expectedByRule {
		s.ViolationRecall[id] = ratio(foundByRule[id], n)
	}
	if s.Cases > 0 {
		s.ProcessingMillis /= float64(s.Cases)
	}
	s.TopologyEdgeAccuracy = ratio(edgeFound, edgeExpected+edgeObserved-edgeFound)
	if edgeExpected == 0 && edgeObserved == 0 {
		s.TopologyEdgeAccuracy = 1
	}
	s.NormalFalsePositiveRate = ratio(normal, totalNormal)
	s.PeakAllocatedBytes = peak
	return s, nil
}
func matchedEdges(expected, observed []model.Edge) int {
	matched := 0
	for _, x := range expected {
		for _, o := range observed {
			if x.Producer == o.Producer && x.Consumer == o.Consumer && x.System == o.System && x.Destination == o.Destination {
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
