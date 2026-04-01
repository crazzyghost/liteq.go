// Package liteq provides a PostgreSQL-backed task queue with retry and dead-letter support.
package liteq

import (
	"fmt"
	"strings"

	"github.com/stephenafamo/bob"
	"github.com/stephenafamo/bob/dialect/psql"
	"github.com/stephenafamo/bob/dialect/psql/dialect"
	"github.com/stephenafamo/bob/dialect/psql/sm"
	"github.com/stephenafamo/bob/dialect/psql/um"
)

// ConditionOperator represents a SQL comparison or logical operator.
type ConditionOperator string

// Supported SQL operators for use in Condition filters.
const (
	OpEqual       ConditionOperator = "="
	OpNotEqual    ConditionOperator = "!="
	OpGreaterThan ConditionOperator = ">"
	OpLessThan    ConditionOperator = "<"
	OpGreaterEq   ConditionOperator = ">="
	OpLessEq      ConditionOperator = "<="
	OpLike        ConditionOperator = "LIKE"
	OpIsNull      ConditionOperator = "IS NULL"
	OpIsNotNull   ConditionOperator = "IS NOT NULL"
	OpIn          ConditionOperator = "IN"
	OpNotIn       ConditionOperator = "NOT IN"
	OpAnd         ConditionOperator = "AND"
	OpOr          ConditionOperator = "OR"
	OpNot         ConditionOperator = "NOT"
	OpExists      ConditionOperator = "EXISTS"
	OpNotExists   ConditionOperator = "NOT EXISTS"
	OpBetween     ConditionOperator = "BETWEEN"
	OpNotBetween  ConditionOperator = "NOT BETWEEN"
	OpILike       ConditionOperator = "ILIKE"
	OpNotILike    ConditionOperator = "NOT ILIKE"
	OpIRegexp     ConditionOperator = "~*"
	OpNotIRegexp  ConditionOperator = "!~*"
	OpRegexp      ConditionOperator = "~"
	OpNotRegexp   ConditionOperator = "!~"
	OpJSONGet     ConditionOperator = "->"
	OpJSONGetText ConditionOperator = "->>"
)

// Condition describes a single WHERE-clause filter for queue task queries.
type Condition struct {
	Field    string            // e.g., "data", "id", "status", "deleted_at"
	Keys     []string          // JSON path segments, e.g. ["config", "settings", "theme"]
	JSONText bool              // If true, last key uses ->> (text extraction); if false, uses -> (JSON)
	Operator ConditionOperator // The comparison operator
	Value    any               // nil for IS NULL/IS NOT NULL operators
}

// NewCondition creates a Condition for a plain column comparison.
func NewCondition(field string, op ConditionOperator, value any) Condition {
	return Condition{
		Field:    field,
		Operator: op,
		Value:    value,
	}
}

// NewJSONCondition creates a Condition that extracts a single JSON key as text.
func NewJSONCondition(field, key string, op ConditionOperator, value any) Condition {
	return Condition{
		Field:    field,
		Keys:     []string{key},
		JSONText: true,
		Operator: op,
		Value:    value,
	}
}

// NewJSONPathCondition creates a Condition that traverses a multi-segment JSON path.
func NewJSONPathCondition(field string, keys []string, op ConditionOperator, value any, jsonObj bool) Condition {
	return Condition{
		Field:    field,
		Keys:     keys,
		JSONText: !jsonObj,
		Operator: op,
		Value:    value,
	}
}

// DataCondition creates a Condition for a JSON text extraction on the data column.
func DataCondition(key string, op ConditionOperator, value any) Condition {
	return NewJSONCondition("data", key, op, value)
}

// DataPathCondition creates a Condition for a deep JSON text path on the data column.
func DataPathCondition(keys []string, op ConditionOperator, value any) Condition {
	return NewJSONPathCondition("data", keys, op, value, false)
}

// DataJSONCondition creates a Condition for a JSON object path on the data column.
func DataJSONCondition(keys []string, op ConditionOperator, value any) Condition {
	return NewJSONPathCondition("data", keys, op, value, true)
}

// DataEquals creates a Condition that checks equality on a data JSON key.
func DataEquals(key string, value any) Condition {
	return DataCondition(key, OpEqual, value)
}

// DataNotEquals creates a Condition that checks inequality on a data JSON key.
func DataNotEquals(key string, value any) Condition {
	return DataCondition(key, OpNotEqual, value)
}

// DataIsNotNull creates a Condition that checks a data JSON key is not null.
func DataIsNotNull(key string) Condition {
	return DataCondition(key, OpIsNotNull, nil)
}

// DataIsNull creates a Condition that checks a data JSON key is null.
func DataIsNull(key string) Condition {
	return DataCondition(key, OpIsNull, nil)
}

// ColumnEquals creates a Condition that checks equality on a plain column.
func ColumnEquals(column string, value any) Condition {
	return NewCondition(column, OpEqual, value)
}

// StatusEquals creates a Condition that filters tasks by status.
func StatusEquals(status string) Condition {
	return ColumnEquals("status", status)
}

// DeletedAtIsNull creates a Condition that filters for non-deleted tasks.
func DeletedAtIsNull() Condition {
	return NewCondition("deleted_at", OpIsNull, nil)
}

// IDEquals creates a Condition that matches a task by its ID.
func IDEquals(id any) Condition {
	return ColumnEquals("id", id)
}

func (c *Condition) getExpression() psql.Expression {
	if len(c.Keys) == 0 {
		return psql.Quote(c.Field)
	}
	var b strings.Builder
	fmt.Fprintf(&b, `"%s"`, c.Field)
	for i, key := range c.Keys {
		if i == len(c.Keys)-1 && c.JSONText {
			fmt.Fprintf(&b, `->>'%s'`, key)
		} else {
			fmt.Fprintf(&b, `->'%s'`, key)
		}
	}
	return psql.Raw(b.String())
}

// SelectMod is a query modifier for PostgreSQL SELECT statements.
type SelectMod = bob.Mod[*dialect.SelectQuery]

// UpdateMod is a query modifier for PostgreSQL UPDATE statements.
type UpdateMod = bob.Mod[*dialect.UpdateQuery]

// SelectQuery is a SELECT query builder that accepts SelectMod modifiers.
type SelectQuery interface {
	Apply(mods ...SelectMod)
}

// UpdateQuery is an UPDATE query builder that accepts UpdateMod modifiers.
type UpdateQuery interface {
	Apply(mods ...UpdateMod)
}

// ApplyToSelect adds this condition as a WHERE clause to a SELECT query.
func (c *Condition) ApplyToSelect(query SelectQuery) {
	expr := c.getExpression()

	switch c.Operator {
	case OpEqual:
		query.Apply(sm.Where(expr.EQ(psql.Arg(c.Value))))
	case OpNotEqual:
		query.Apply(sm.Where(expr.NE(psql.Arg(c.Value))))
	case OpGreaterThan:
		query.Apply(sm.Where(expr.GT(psql.Arg(c.Value))))
	case OpLessThan:
		query.Apply(sm.Where(expr.LT(psql.Arg(c.Value))))
	case OpGreaterEq:
		query.Apply(sm.Where(expr.GTE(psql.Arg(c.Value))))
	case OpLessEq:
		query.Apply(sm.Where(expr.LTE(psql.Arg(c.Value))))
	case OpLike:
		query.Apply(sm.Where(expr.Like(psql.Arg(c.Value))))
	case OpIsNull:
		query.Apply(sm.Where(expr.IsNull()))
	case OpIsNotNull:
		query.Apply(sm.Where(expr.IsNotNull()))
	case OpIn:
		query.Apply(sm.Where(expr.In(psql.Arg(c.Value))))
	case OpNotIn:
		query.Apply(sm.Where(expr.NotIn(psql.Arg(c.Value))))
	default:
		query.Apply(sm.Where(expr.EQ(psql.Arg(c.Value))))
	}
}

// ApplyToUpdate adds this condition as a WHERE clause to an UPDATE query.
func (c *Condition) ApplyToUpdate(query UpdateQuery) {
	expr := c.getExpression()

	switch c.Operator {
	case OpEqual:
		query.Apply(um.Where(expr.EQ(psql.Arg(c.Value))))
	case OpNotEqual:
		query.Apply(um.Where(expr.NE(psql.Arg(c.Value))))
	case OpGreaterThan:
		query.Apply(um.Where(expr.GT(psql.Arg(c.Value))))
	case OpLessThan:
		query.Apply(um.Where(expr.LT(psql.Arg(c.Value))))
	case OpGreaterEq:
		query.Apply(um.Where(expr.GTE(psql.Arg(c.Value))))
	case OpLessEq:
		query.Apply(um.Where(expr.LTE(psql.Arg(c.Value))))
	case OpLike:
		query.Apply(um.Where(expr.Like(psql.Arg(c.Value))))
	case OpIsNull:
		query.Apply(um.Where(expr.IsNull()))
	case OpIsNotNull:
		query.Apply(um.Where(expr.IsNotNull()))
	case OpIn:
		query.Apply(um.Where(expr.In(psql.Arg(c.Value))))
	case OpNotIn:
		query.Apply(um.Where(expr.NotIn(psql.Arg(c.Value))))
	default:
		query.Apply(um.Where(expr.EQ(psql.Arg(c.Value))))
	}
}
