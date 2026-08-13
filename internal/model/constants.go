package model

// Correlation and evidence methods are shared between correlation, rules, and
// reports. Keeping them here prevents subtle drift in serialized output.
const (
	MethodSpanLink           = "span_link"
	MethodParentContext      = "parent_context"
	MethodMessagingAttrs     = "messaging_attributes"
	MethodKafkaPartOffset    = "kafka_partition_offset"
	MethodTimeHeuristic      = "time_window_heuristic"
	MethodNone               = "none"
	MethodCorrelatedTopology = "correlated_topology"
	MethodExpectedTopology   = "expected_topology"
	MethodExpectedDelivery   = "expected_delivery"
	MethodSemanticValidation = "semantic_validation"
	MethodIdentityCandidate  = "identity_candidate"
	MethodTimeCandidate      = "time_candidate"
	MethodUnresolvedRef      = "unresolved_context_reference"
)

const (
	CoverageComplete = "complete"
	CoverageUnknown  = "unknown"
	CoverageDegraded = "degraded"
)
