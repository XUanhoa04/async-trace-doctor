package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	v, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", node.Value, err)
	}
	d.Duration = v
	return nil
}

type Config struct {
	APIVersion                string   `yaml:"apiVersion"`
	SemanticConventionVersion string   `yaml:"semanticConventionVersion"`
	Settings                  Settings `yaml:"settings"`
	RedactAttributes          []string `yaml:"redactAttributes"`
	Rules                     []Rule   `yaml:"rules"`
	Topology                  Topology `yaml:"topology"`
}

type Settings struct {
	CorrelationWindow  Duration `yaml:"correlationWindow"`
	QueueLatency       Duration `yaml:"queueLatencyThreshold"`
	ClockSkewTolerance Duration `yaml:"clockSkewTolerance"`
	DuplicateThreshold int      `yaml:"duplicateThreshold"`
	FailOnSeverity     string   `yaml:"failOnSeverity"`
}

type Rule struct {
	ID           string    `yaml:"id"`
	Check        string    `yaml:"check"`
	Enabled      bool      `yaml:"enabled"`
	Severity     string    `yaml:"severity"`
	Message      string    `yaml:"message"`
	SuggestedFix string    `yaml:"suggestedFix"`
	AppliesTo    AppliesTo `yaml:"appliesTo"`
}

type AppliesTo struct {
	Operations []string `yaml:"operations"`
}

type Topology struct {
	ExpectedEdges []ExpectedEdge `yaml:"expectedEdges"`
	DeniedEdges   []ExpectedEdge `yaml:"deniedEdges"`
	IgnoredEdges  []ExpectedEdge `yaml:"ignoredEdges"`
}
type ExpectedEdge struct {
	Producer          string `yaml:"producer"`
	System            string `yaml:"system"`
	Destination       string `yaml:"destination"`
	Consumer          string `yaml:"consumer"`
	ConsumerGroup     string `yaml:"consumerGroup,omitempty"`
	Subscription      string `yaml:"subscription,omitempty"`
	RequirePerMessage bool   `yaml:"requirePerMessage,omitempty"`
}

var validChecks = map[string]bool{
	"missing_service_name": true, "missing_messaging_system": true, "missing_destination": true,
	"invalid_operation": true, "invalid_span_kind": true, "missing_consumer_context": true,
	"orphan_producer": true, "orphan_consumer": true, "batch_links_incomplete": true,
	"duplicate_processing": true, "queue_latency_high": true, "clock_skew": true,
	"runtime_topology_mismatch": true,
}
var severityRank = map[string]int{"info": 0, "warning": 1, "error": 2, "critical": 3}

func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read rules config: %w", err)
	}
	var c Config
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("decode rules config: %w", err)
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c Config) Validate() error {
	if c.APIVersion != "asynctracedoctor.io/v1alpha1" {
		return fmt.Errorf("unsupported apiVersion %q", c.APIVersion)
	}
	if c.SemanticConventionVersion == "" {
		return fmt.Errorf("semanticConventionVersion is required")
	}
	if c.Settings.CorrelationWindow.Duration <= 0 {
		return fmt.Errorf("correlationWindow must be positive")
	}
	if c.Settings.QueueLatency.Duration <= 0 {
		return fmt.Errorf("queueLatencyThreshold must be positive")
	}
	if c.Settings.ClockSkewTolerance.Duration < 0 {
		return fmt.Errorf("clockSkewTolerance must not be negative")
	}
	if c.Settings.DuplicateThreshold < 1 {
		return fmt.Errorf("duplicateThreshold must be at least 1")
	}
	if _, ok := severityRank[c.Settings.FailOnSeverity]; !ok {
		return fmt.Errorf("invalid failOnSeverity %q", c.Settings.FailOnSeverity)
	}
	seen := map[string]bool{}
	for i, r := range c.Rules {
		if r.ID == "" || seen[r.ID] {
			return fmt.Errorf("rule %d has empty or duplicate id %q", i, r.ID)
		}
		seen[r.ID] = true
		if !validChecks[r.Check] {
			return fmt.Errorf("rule %q has unknown check %q", r.ID, r.Check)
		}
		if _, ok := severityRank[r.Severity]; !ok {
			return fmt.Errorf("rule %q has invalid severity %q", r.ID, r.Severity)
		}
		if r.Message == "" || r.SuggestedFix == "" {
			return fmt.Errorf("rule %q requires message and suggestedFix", r.ID)
		}
	}
	topologySets := []struct {
		kind  string
		edges []ExpectedEdge
	}{{"expected", c.Topology.ExpectedEdges}, {"denied", c.Topology.DeniedEdges}, {"ignored", c.Topology.IgnoredEdges}}
	for _, set := range topologySets {
		for i, edge := range set.edges {
			if edge.Producer == "" || edge.System == "" || edge.Destination == "" || edge.Consumer == "" {
				return fmt.Errorf("topology %s edge %d requires producer, system, destination, and consumer", set.kind, i)
			}
			if set.kind != "expected" && edge.RequirePerMessage {
				return fmt.Errorf("topology %s edge %d cannot require per-message delivery", set.kind, i)
			}
		}
	}
	return nil
}

func (c Config) ViolatesPolicy(severity string) bool {
	return severityRank[severity] >= severityRank[c.Settings.FailOnSeverity]
}
