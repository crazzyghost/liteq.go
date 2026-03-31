package liteq

import (
	"context"
	"strings"
	"testing"

	"github.com/stephenafamo/bob/dialect/psql"
	"github.com/stephenafamo/bob/dialect/psql/sm"
)

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

func TestNewCondition(t *testing.T) {
	c := NewCondition("id", OpEqual, "abc")
	if c.Field != "id" || c.Operator != OpEqual || c.Value != "abc" {
		t.Fatalf("unexpected condition: %+v", c)
	}
	if len(c.Keys) != 0 {
		t.Fatalf("expected no keys, got %v", c.Keys)
	}
}

func TestNewJSONCondition(t *testing.T) {
	c := NewJSONCondition("data", "type", OpEqual, "email")
	if c.Field != "data" || len(c.Keys) != 1 || c.Keys[0] != "type" || !c.JsonText {
		t.Fatalf("unexpected condition: %+v", c)
	}
}

func TestNewJSONPathConditionVariants(t *testing.T) {
	textCond := NewJSONPathCondition("data", []string{"retryPolicy", "strategy"}, OpEqual, "fixed", false)
	if !textCond.JsonText {
		t.Fatalf("expected text JSON path condition, got %+v", textCond)
	}

	jsonCond := NewJSONPathCondition("data", []string{"config"}, OpEqual, nil, true)
	if jsonCond.JsonText {
		t.Fatalf("expected JSON object path condition, got %+v", jsonCond)
	}
}

func TestDataHelpers(t *testing.T) {
	cases := []struct {
		name string
		cond Condition
		want ConditionOperator
	}{
		{name: "equals", cond: DataEquals("priority", 1), want: OpEqual},
		{name: "not-equals", cond: DataNotEquals("type", "scheduled"), want: OpNotEqual},
		{name: "is-null", cond: DataIsNull("deletedAt"), want: OpIsNull},
		{name: "is-not-null", cond: DataIsNotNull("completedAt"), want: OpIsNotNull},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.cond.Field != "data" || tc.cond.Operator != tc.want {
				t.Fatalf("unexpected condition: %+v", tc.cond)
			}
		})
	}
}

func TestColumnHelpers(t *testing.T) {
	status := StatusEquals("RUNNING")
	if status.Field != "status" || len(status.Keys) != 0 || status.Value != "RUNNING" {
		t.Fatalf("unexpected status condition: %+v", status)
	}

	deletedAt := DeletedAtIsNull()
	if deletedAt.Field != "deleted_at" || deletedAt.Operator != OpIsNull {
		t.Fatalf("unexpected deleted_at condition: %+v", deletedAt)
	}

	id := IDEquals("id-999")
	if id.Field != "id" || id.Operator != OpEqual || id.Value != "id-999" {
		t.Fatalf("unexpected id condition: %+v", id)
	}
}

func TestDataPathHelpers(t *testing.T) {
	pathCond := DataPathCondition([]string{"a", "b", "c"}, OpEqual, "v")
	if pathCond.Field != "data" || len(pathCond.Keys) != 3 || !pathCond.JsonText {
		t.Fatalf("unexpected data path condition: %+v", pathCond)
	}

	jsonCond := DataJsonCondition([]string{"config"}, OpIsNotNull, nil)
	if jsonCond.Field != "data" || jsonCond.JsonText {
		t.Fatalf("unexpected data JSON condition: %+v", jsonCond)
	}
}

func TestApplyToSelect_SimpleColumn(t *testing.T) {
	sql, args := buildSelectSQL(t, IDEquals("abc"))
	if !strings.Contains(sql, `"id"`) {
		t.Fatalf("SQL missing quoted id column: %s", sql)
	}
	if len(args) != 1 || args[0] != "abc" {
		t.Fatalf("args = %v, want [abc]", args)
	}
}

func TestApplyToSelect_StatusColumn(t *testing.T) {
	sql, args := buildSelectSQL(t, StatusEquals("PENDING"))
	if !strings.Contains(sql, `"status" =`) {
		t.Fatalf("SQL should filter native status column, got: %s", sql)
	}
	if len(args) != 1 || args[0] != "PENDING" {
		t.Fatalf("args = %v, want [PENDING]", args)
	}
}

func TestApplyToSelect_JSONPathText(t *testing.T) {
	sql, _ := buildSelectSQL(t, DataCondition("status", OpEqual, "active"))
	if !strings.Contains(sql, `"data"->>'status'`) {
		t.Fatalf("SQL should contain JSON text extraction, got: %s", sql)
	}
}

func TestApplyToSelect_JSONPathJSON(t *testing.T) {
	sql, _ := buildSelectSQL(t, DataJsonCondition([]string{"retryPolicy"}, OpIsNotNull, nil))
	if !strings.Contains(sql, `"data"->'retryPolicy'`) {
		t.Fatalf("SQL should use -> for JSON object extraction, got: %s", sql)
	}
}

func TestApplyToSelect_DeepJSONPath(t *testing.T) {
	sql, args := buildSelectSQL(t, DataPathCondition([]string{"config", "settings", "theme"}, OpEqual, "dark"))
	if !strings.Contains(sql, `"data"->'config'->'settings'->>'theme'`) {
		t.Fatalf("SQL missing deep JSON path: %s", sql)
	}
	if len(args) != 1 || args[0] != "dark" {
		t.Fatalf("args = %v, want [dark]", args)
	}
}

func TestApplyToSelect_Operators(t *testing.T) {
	cases := []Condition{
		NewCondition("created_at", OpLessThan, "2024-01-01"),
		NewCondition("created_at", OpGreaterEq, "2024-01-01"),
		NewCondition("created_at", OpLessEq, "2024-12-31"),
		NewCondition("id", OpLike, "prefix%"),
		NewCondition("id", OpIn, []string{"a", "b", "c"}),
		NewCondition("id", OpNotIn, []string{"x"}),
	}

	for _, cond := range cases {
		sql, _ := buildSelectSQL(t, cond)
		if sql == "" {
			t.Fatalf("expected SQL for condition %+v", cond)
		}
	}
}

func TestApplyToSelect_IsNull(t *testing.T) {
	sql, args := buildSelectSQL(t, DeletedAtIsNull())
	if !strings.Contains(sql, "IS NULL") {
		t.Fatalf("SQL missing IS NULL: %s", sql)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %v", args)
	}
}

func TestApplyToSelect_UnknownOperatorFallsBackToEqual(t *testing.T) {
	sql, _ := buildSelectSQL(t, Condition{Field: "id", Operator: "UNKNOWN_OP", Value: "val"})
	if sql == "" {
		t.Fatal("expected non-empty SQL")
	}
}

func TestApplyToUpdate_StatusColumn(t *testing.T) {
	sql, args := buildUpdateSQL(t, StatusEquals("PENDING"))
	if !strings.Contains(sql, `"status" =`) {
		t.Fatalf("UPDATE SQL should filter native status column, got: %s", sql)
	}
	if len(args) != 1 || args[0] != "PENDING" {
		t.Fatalf("args = %v, want [PENDING]", args)
	}
}

func TestApplyToUpdate_DataPathAndNull(t *testing.T) {
	jsonSQL, _ := buildUpdateSQL(t, DataCondition("type", OpNotEqual, "scheduled"))
	if !strings.Contains(jsonSQL, `"data"->>'type'`) {
		t.Fatalf("UPDATE SQL should contain JSON text extraction, got: %s", jsonSQL)
	}

	nullSQL, args := buildUpdateSQL(t, DeletedAtIsNull())
	if !strings.Contains(nullSQL, "IS NULL") {
		t.Fatalf("UPDATE SQL missing IS NULL: %s", nullSQL)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %v", args)
	}
}
