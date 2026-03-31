package liteq

import (
	"context"
	"strings"
	"testing"

	"github.com/stephenafamo/bob/dialect/psql"
	"github.com/stephenafamo/bob/dialect/psql/sm"
)

// buildSelectSQL is a test helper that applies a Condition to a SELECT against
// "queue_tasks" and returns the rendered SQL and its arguments.
func buildSelectSQL(t *testing.T, c Condition) (string, []any) {
	t.Helper()
	q := psql.Select(sm.From("queue_tasks"))
	c.ApplyToSelect(q)
	sql, args, err := q.Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	return sql, args
}

// buildUpdateSQL applies a Condition to an UPDATE of "queue_tasks" and returns
// the rendered SQL and its arguments.
func buildUpdateSQL(t *testing.T, c Condition) (string, []any) {
	t.Helper()
	q := psql.Update()
	c.ApplyToUpdate(q)
	sql, args, err := q.Build(context.Background())
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	return sql, args
}

// ---- Constructor / field tests ----

func TestNewCondition(t *testing.T) {
	c := NewCondition("id", OpEqual, "abc")
	if c.Field != "id" {
		t.Errorf("Field = %q, want %q", c.Field, "id")
	}
	if c.Operator != OpEqual {
		t.Errorf("Operator = %q, want %q", c.Operator, OpEqual)
	}
	if c.Value != "abc" {
		t.Errorf("Value = %v, want %q", c.Value, "abc")
	}
	if len(c.Keys) != 0 {
		t.Errorf("Keys should be empty, got %v", c.Keys)
	}
}

func TestNewJSONCondition(t *testing.T) {
	c := NewJSONCondition("data", "type", OpEqual, "email")
	if c.Field != "data" {
		t.Errorf("Field = %q, want %q", c.Field, "data")
	}
	if len(c.Keys) != 1 || c.Keys[0] != "type" {
		t.Errorf("Keys = %v, want [type]", c.Keys)
	}
	if !c.JsonText {
		t.Error("JsonText should be true for single-key JSON condition")
	}
	if c.Operator != OpEqual {
		t.Errorf("Operator = %q, want %q", c.Operator, OpEqual)
	}
}

func TestNewJSONPathCondition_Text(t *testing.T) {
	c := NewJSONPathCondition("meta", []string{"retryPolicy", "strategy"}, OpEqual, "fixed", false)
	if !c.JsonText {
		t.Error("JsonText should be true when jsonObj=false")
	}
	if len(c.Keys) != 2 {
		t.Errorf("Keys len = %d, want 2", len(c.Keys))
	}
}

func TestNewJSONPathCondition_JSON(t *testing.T) {
	c := NewJSONPathCondition("data", []string{"config"}, OpEqual, nil, true)
	if c.JsonText {
		t.Error("JsonText should be false when jsonObj=true")
	}
}

func TestDataCondition(t *testing.T) {
	c := DataCondition("status", OpEqual, "active")
	if c.Field != "data" {
		t.Errorf("Field = %q, want %q", c.Field, "data")
	}
	if c.Keys[0] != "status" {
		t.Errorf("Keys[0] = %q, want %q", c.Keys[0], "status")
	}
}

func TestMetaCondition(t *testing.T) {
	c := MetaCondition("isRetry", OpEqual, true)
	if c.Field != "meta" {
		t.Errorf("Field = %q, want %q", c.Field, "meta")
	}
	if c.Keys[0] != "isRetry" {
		t.Errorf("Keys[0] = %q, want %q", c.Keys[0], "isRetry")
	}
}

func TestDataEquals(t *testing.T) {
	c := DataEquals("priority", 1)
	if c.Operator != OpEqual {
		t.Errorf("Operator = %q, want %q", c.Operator, OpEqual)
	}
	if c.Value != 1 {
		t.Errorf("Value = %v, want 1", c.Value)
	}
}

func TestMetaEquals(t *testing.T) {
	c := MetaEquals("status", "PENDING")
	if c.Field != "meta" || c.Keys[0] != "status" || c.Value != "PENDING" {
		t.Errorf("MetaEquals produced unexpected condition: %+v", c)
	}
}

func TestDataNotEquals(t *testing.T) {
	c := DataNotEquals("type", "scheduled")
	if c.Operator != OpNotEqual {
		t.Errorf("Operator = %q, want %q", c.Operator, OpNotEqual)
	}
}

func TestMetaNotEquals(t *testing.T) {
	c := MetaNotEquals("status", "FAILED")
	if c.Operator != OpNotEqual {
		t.Errorf("Operator = %q, want %q", c.Operator, OpNotEqual)
	}
}

func TestDataIsNull(t *testing.T) {
	c := DataIsNull("deletedAt")
	if c.Operator != OpIsNull {
		t.Errorf("Operator = %q, want %q", c.Operator, OpIsNull)
	}
	if c.Value != nil {
		t.Errorf("Value should be nil, got %v", c.Value)
	}
}

func TestDataIsNotNull(t *testing.T) {
	c := DataIsNotNull("completedAt")
	if c.Operator != OpIsNotNull {
		t.Errorf("Operator = %q, want %q", c.Operator, OpIsNotNull)
	}
}

func TestMetaIsNull(t *testing.T) {
	c := MetaIsNull("nextRunAt")
	if c.Operator != OpIsNull {
		t.Errorf("Operator = %q, want %q", c.Operator, OpIsNull)
	}
}

func TestMetaIsNotNull(t *testing.T) {
	c := MetaIsNotNull("processedAt")
	if c.Operator != OpIsNotNull {
		t.Errorf("Operator = %q, want %q", c.Operator, OpIsNotNull)
	}
}

func TestColumnEquals(t *testing.T) {
	c := ColumnEquals("id", "uuid-123")
	if c.Field != "id" || c.Operator != OpEqual || c.Value != "uuid-123" {
		t.Errorf("ColumnEquals produced unexpected condition: %+v", c)
	}
	if len(c.Keys) != 0 {
		t.Errorf("Keys should be empty for column condition, got %v", c.Keys)
	}
}

func TestStatusEquals(t *testing.T) {
	c := StatusEquals("RUNNING")
	if c.Field != "meta" || c.Keys[0] != "status" || c.Value != "RUNNING" {
		t.Errorf("StatusEquals produced unexpected condition: %+v", c)
	}
}

func TestDeletedAtIsNull(t *testing.T) {
	c := DeletedAtIsNull()
	if c.Field != "deleted_at" || c.Operator != OpIsNull {
		t.Errorf("DeletedAtIsNull produced unexpected condition: %+v", c)
	}
}

func TestIDEquals(t *testing.T) {
	c := IDEquals("id-999")
	if c.Field != "id" || c.Operator != OpEqual || c.Value != "id-999" {
		t.Errorf("IDEquals produced unexpected condition: %+v", c)
	}
}

func TestDataPathCondition(t *testing.T) {
	c := DataPathCondition([]string{"a", "b", "c"}, OpEqual, "v")
	if c.Field != "data" || len(c.Keys) != 3 || !c.JsonText {
		t.Errorf("DataPathCondition produced unexpected: %+v", c)
	}
}

func TestMetaPathCondition(t *testing.T) {
	c := MetaPathCondition([]string{"retryPolicy", "maxRetries"}, OpGreaterThan, 0)
	if c.Field != "meta" || c.Operator != OpGreaterThan {
		t.Errorf("MetaPathCondition produced unexpected: %+v", c)
	}
}

func TestDataJsonCondition(t *testing.T) {
	c := DataJsonCondition([]string{"config"}, OpIsNotNull, nil)
	if c.JsonText {
		t.Error("DataJsonCondition should have JsonText=false")
	}
}

func TestMetaJsonCondition(t *testing.T) {
	c := MetaJsonCondition([]string{"retryPolicy"}, OpIsNotNull, nil)
	if c.Field != "meta" || c.JsonText {
		t.Errorf("MetaJsonCondition produced unexpected: %+v", c)
	}
}

// ---- SQL generation tests ----

func TestApplyToSelect_SimpleColumn_Equal(t *testing.T) {
	c := IDEquals("abc")
	sql, args := buildSelectSQL(t, c)
	if !strings.Contains(sql, `"id"`) {
		t.Errorf("SQL missing quoted column: %s", sql)
	}
	if len(args) != 1 || args[0] != "abc" {
		t.Errorf("args = %v, want [abc]", args)
	}
}

func TestApplyToSelect_JSONPath_Text(t *testing.T) {
	c := MetaEquals("status", "PENDING")
	sql, _ := buildSelectSQL(t, c)
	if !strings.Contains(sql, `"meta"->>'status'`) {
		t.Errorf("SQL should contain JSON text extraction, got: %s", sql)
	}
}

func TestApplyToSelect_JSONPath_JSON(t *testing.T) {
	c := MetaJsonCondition([]string{"retryPolicy"}, OpIsNotNull, nil)
	sql, _ := buildSelectSQL(t, c)
	// Should use -> (not ->>) for JSON object extraction.
	if !strings.Contains(sql, `"meta"->'retryPolicy'`) {
		t.Errorf("SQL should use -> for JSON object, got: %s", sql)
	}
}

func TestApplyToSelect_IsNull(t *testing.T) {
	c := DeletedAtIsNull()
	sql, args := buildSelectSQL(t, c)
	if !strings.Contains(sql, "IS NULL") {
		t.Errorf("SQL missing IS NULL: %s", sql)
	}
	if len(args) != 0 {
		t.Errorf("IS NULL should produce no args, got: %v", args)
	}
}

func TestApplyToSelect_IsNotNull(t *testing.T) {
	c := DataIsNotNull("completedAt")
	sql, _ := buildSelectSQL(t, c)
	if !strings.Contains(sql, "IS NOT NULL") {
		t.Errorf("SQL missing IS NOT NULL: %s", sql)
	}
}

func TestApplyToSelect_DeepJSONPath(t *testing.T) {
	c := DataPathCondition([]string{"config", "settings", "theme"}, OpEqual, "dark")
	sql, args := buildSelectSQL(t, c)
	if !strings.Contains(sql, `"data"->'config'->'settings'->>'theme'`) {
		t.Errorf("SQL missing deep JSON path: %s", sql)
	}
	if len(args) != 1 || args[0] != "dark" {
		t.Errorf("args = %v, want [dark]", args)
	}
}

func TestApplyToSelect_NotEqual(t *testing.T) {
	c := DataNotEquals("type", "scheduled")
	sql, _ := buildSelectSQL(t, c)
	if !strings.Contains(sql, "!=") && !strings.Contains(sql, "<>") {
		t.Errorf("SQL missing != / <> operator: %s", sql)
	}
}

func TestApplyToSelect_GreaterThan(t *testing.T) {
	c := MetaPathCondition([]string{"retries"}, OpGreaterThan, 2)
	sql, args := buildSelectSQL(t, c)
	if !strings.Contains(sql, ">") {
		t.Errorf("SQL missing > operator: %s", sql)
	}
	if len(args) != 1 || args[0] != 2 {
		t.Errorf("args = %v, want [2]", args)
	}
}

func TestApplyToSelect_LessThan(t *testing.T) {
	c := NewCondition("created_at", OpLessThan, "2024-01-01")
	sql, _ := buildSelectSQL(t, c)
	if !strings.Contains(sql, "<") {
		t.Errorf("SQL missing < operator: %s", sql)
	}
}

func TestApplyToSelect_GreaterEq(t *testing.T) {
	c := NewCondition("created_at", OpGreaterEq, "2024-01-01")
	sql, _ := buildSelectSQL(t, c)
	if !strings.Contains(sql, ">=") {
		t.Errorf("SQL missing >= operator: %s", sql)
	}
}

func TestApplyToSelect_LessEq(t *testing.T) {
	c := NewCondition("created_at", OpLessEq, "2024-12-31")
	sql, _ := buildSelectSQL(t, c)
	if !strings.Contains(sql, "<=") {
		t.Errorf("SQL missing <= operator: %s", sql)
	}
}

func TestApplyToSelect_Like(t *testing.T) {
	c := NewCondition("id", OpLike, "prefix%")
	sql, _ := buildSelectSQL(t, c)
	if !strings.Contains(sql, "LIKE") {
		t.Errorf("SQL missing LIKE: %s", sql)
	}
}

func TestApplyToSelect_In(t *testing.T) {
	c := NewCondition("id", OpIn, []string{"a", "b", "c"})
	sql, _ := buildSelectSQL(t, c)
	if !strings.Contains(sql, "IN") {
		t.Errorf("SQL missing IN: %s", sql)
	}
}

func TestApplyToSelect_NotIn(t *testing.T) {
	c := NewCondition("id", OpNotIn, []string{"x"})
	sql, _ := buildSelectSQL(t, c)
	if !strings.Contains(sql, "NOT IN") {
		t.Errorf("SQL missing NOT IN: %s", sql)
	}
}

func TestApplyToSelect_UnknownOperator_FallsBackToEqual(t *testing.T) {
	// Unknown operator should default to = without panic.
	c := Condition{Field: "id", Operator: "UNKNOWN_OP", Value: "val"}
	sql, _ := buildSelectSQL(t, c)
	if sql == "" {
		t.Error("Should produce non-empty SQL for unknown operator fallback")
	}
}

// ---- ApplyToUpdate SQL generation tests ----

func TestApplyToUpdate_Equal(t *testing.T) {
	c := IDEquals("uuid-1")
	sql, args := buildUpdateSQL(t, c)
	if !strings.Contains(sql, `"id"`) {
		t.Errorf("UPDATE SQL missing quoted column: %s", sql)
	}
	if len(args) == 0 {
		t.Error("UPDATE args should not be empty")
	}
	_ = args
}

func TestApplyToUpdate_JSONPath(t *testing.T) {
	c := MetaEquals("status", "PENDING")
	sql, _ := buildUpdateSQL(t, c)
	if !strings.Contains(sql, `"meta"->>'status'`) {
		t.Errorf("UPDATE SQL should contain JSON text extraction, got: %s", sql)
	}
}

func TestApplyToUpdate_IsNull(t *testing.T) {
	c := DeletedAtIsNull()
	sql, args := buildUpdateSQL(t, c)
	if !strings.Contains(sql, "IS NULL") {
		t.Errorf("UPDATE SQL missing IS NULL: %s", sql)
	}
	if len(args) != 0 {
		t.Errorf("IS NULL should produce no args, got: %v", args)
	}
}

func TestApplyToUpdate_IsNotNull(t *testing.T) {
	c := DataIsNotNull("completedAt")
	sql, _ := buildUpdateSQL(t, c)
	if !strings.Contains(sql, "IS NOT NULL") {
		t.Errorf("UPDATE SQL missing IS NOT NULL: %s", sql)
	}
}

func TestApplyToUpdate_NotEqual(t *testing.T) {
	c := MetaNotEquals("status", "CANCELLED")
	sql, _ := buildUpdateSQL(t, c)
	if !strings.Contains(sql, "!=") && !strings.Contains(sql, "<>") {
		t.Errorf("UPDATE SQL missing != / <> operator: %s", sql)
	}
}

func TestApplyToUpdate_GreaterThan(t *testing.T) {
	c := NewCondition("id", OpGreaterThan, 5)
	sql, _ := buildUpdateSQL(t, c)
	if !strings.Contains(sql, ">") {
		t.Errorf("UPDATE SQL missing > operator: %s", sql)
	}
}

func TestApplyToUpdate_LessThan(t *testing.T) {
	c := NewCondition("id", OpLessThan, 10)
	sql, _ := buildUpdateSQL(t, c)
	if !strings.Contains(sql, "<") {
		t.Errorf("UPDATE SQL missing < operator: %s", sql)
	}
}

func TestApplyToUpdate_GreaterEq(t *testing.T) {
	c := NewCondition("id", OpGreaterEq, 1)
	sql, _ := buildUpdateSQL(t, c)
	if !strings.Contains(sql, ">=") {
		t.Errorf("UPDATE SQL missing >= operator: %s", sql)
	}
}

func TestApplyToUpdate_LessEq(t *testing.T) {
	c := NewCondition("id", OpLessEq, 100)
	sql, _ := buildUpdateSQL(t, c)
	if !strings.Contains(sql, "<=") {
		t.Errorf("UPDATE SQL missing <= operator: %s", sql)
	}
}

func TestApplyToUpdate_Like(t *testing.T) {
	c := NewCondition("id", OpLike, "prefix%")
	sql, _ := buildUpdateSQL(t, c)
	if !strings.Contains(sql, "LIKE") {
		t.Errorf("UPDATE SQL missing LIKE: %s", sql)
	}
}

func TestApplyToUpdate_In(t *testing.T) {
	c := NewCondition("id", OpIn, []string{"a", "b"})
	sql, _ := buildUpdateSQL(t, c)
	if !strings.Contains(sql, "IN") {
		t.Errorf("UPDATE SQL missing IN: %s", sql)
	}
}

func TestApplyToUpdate_NotIn(t *testing.T) {
	c := NewCondition("id", OpNotIn, []string{"x"})
	sql, _ := buildUpdateSQL(t, c)
	if !strings.Contains(sql, "NOT IN") {
		t.Errorf("UPDATE SQL missing NOT IN: %s", sql)
	}
}

func TestApplyToUpdate_UnknownOperator_FallsBackToEqual(t *testing.T) {
	c := Condition{Field: "id", Operator: "UNSUPPORTED", Value: "v"}
	sql, _ := buildUpdateSQL(t, c)
	if sql == "" {
		t.Error("Should produce non-empty SQL for unknown UPDATE operator fallback")
	}
}
