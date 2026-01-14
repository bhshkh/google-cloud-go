package firestore

import (
	"testing"
)

func TestRawAggregate(t *testing.T) {
	// Test creating a raw aggregate
	agg := RawAggregate("custom_agg", FieldOf("a"), ConstantOf(10))
	
	// Check if it implements AggregateFunction
	if _, ok := agg.(AggregateFunction); !ok {
		t.Fatal("RawAggregate does not implement AggregateFunction")
	}

	// Verify Proto generation
	pbVal, err := agg.toProto()
	if err != nil {
		t.Fatalf("toProto failed: %v", err)
	}

	funcVal := pbVal.GetFunctionValue()
	if funcVal == nil {
		t.Fatal("Expected FunctionValue")
	}

	if funcVal.Name != "custom_agg" {
		t.Errorf("Expected name 'custom_agg', got %s", funcVal.Name)
	}

	if len(funcVal.Args) != 2 {
		t.Fatalf("Expected 2 args, got %d", len(funcVal.Args))
	}

	// Check first arg (FieldReference)
	if funcVal.Args[0].GetFieldReferenceValue() != "a" {
		t.Errorf("Expected first arg to be field 'a', got %v", funcVal.Args[0])
	}

	// Check second arg (Integer)
	if funcVal.Args[1].GetIntegerValue() != 10 {
		t.Errorf("Expected second arg to be int 10, got %v", funcVal.Args[1])
	}
}

func TestPipeline_RawAggregate(t *testing.T) {
	client := &Client{projectID: "project", databaseID: "database"}
	ps := &PipelineSource{client: client}

	p := ps.Collection("users").Aggregate(
		RawAggregate("sum_custom", FieldOf("age")).As("total_age"),
	)

	req, err := p.toExecutePipelineRequest()
	if err != nil {
		t.Fatalf("toExecutePipelineRequest: %v", err)
	}

	stages := req.GetStructuredPipeline().GetPipeline().GetStages()
	// Collection + Aggregate
	if len(stages) != 2 {
		t.Fatalf("stages count: got %d, want 2", len(stages))
	}

	aggStage := stages[1]
	if aggStage.Name != "aggregate" {
		t.Errorf("expected aggregate stage, got %s", aggStage.Name)
	}

	// Helper to access aggregation map
	aggs := aggStage.Args[0].GetMapValue().GetFields()
	totalAge := aggs["total_age"]
	if totalAge == nil {
		t.Fatal("missing 'total_age' aggregation")
	}

	funcVal := totalAge.GetFunctionValue()
	if funcVal.Name != "sum_custom" {
		t.Errorf("expected sum_custom, got %s", funcVal.Name)
	}
	
	if len(funcVal.Args) != 1 {
		t.Errorf("expected 1 arg, got %d", len(funcVal.Args))
	}
	
	if funcVal.Args[0].GetFieldReferenceValue() != "age" {
		t.Errorf("expected arg 'age', got %v", funcVal.Args[0])
	}
}
