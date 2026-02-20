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

type ConditionOperator string

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
	OpJsonGet     ConditionOperator = "->"
	OpJsonGetText ConditionOperator = "->>"
)

type Condition struct {
	Field    string            // e.g., "data", "meta", "id", "deleted_at"
	Keys     []string          // JSON path segments, e.g. ["config", "settings", "theme"]
	JsonText bool              // If true, last key uses ->> (text extraction); if false, uses -> (JSON)
	Operator ConditionOperator // The comparison operator
	Value    any               // nil for IS NULL/IS NOT NULL operators
}

func NewCondition(field string, op ConditionOperator, value any) Condition {
	return Condition{
		Field:    field,
		Operator: op,
		Value:    value,
	}
}

func NewJSONCondition(field, key string, op ConditionOperator, value any) Condition {
	return Condition{
		Field:    field,
		Keys:     []string{key},
		JsonText: true,
		Operator: op,
		Value:    value,
	}
}

func NewJSONPathCondition(field string, keys []string, op ConditionOperator, value any, jsonObj bool) Condition {
	return Condition{
		Field:    field,
		Keys:     keys,
		JsonText: !jsonObj,
		Operator: op,
		Value:    value,
	}
}

func DataCondition(key string, op ConditionOperator, value any) Condition {
	return NewJSONCondition("data", key, op, value)
}
func MetaCondition(key string, op ConditionOperator, value any) Condition {
	return NewJSONCondition("meta", key, op, value)
}

func DataPathCondition(keys []string, op ConditionOperator, value any) Condition {
	return NewJSONPathCondition("data", keys, op, value, false)
}
func MetaPathCondition(keys []string, op ConditionOperator, value any) Condition {
	return NewJSONPathCondition("meta", keys, op, value, false)
}

func DataJsonCondition(keys []string, op ConditionOperator, value any) Condition {
	return NewJSONPathCondition("data", keys, op, value, true)
}

func MetaJsonCondition(keys []string, op ConditionOperator, value any) Condition {
	return NewJSONPathCondition("meta", keys, op, value, true)
}

func DataEquals(key string, value any) Condition {
	return DataCondition(key, OpEqual, value)
}

func MetaEquals(key string, value any) Condition {
	return MetaCondition(key, OpEqual, value)
}

func DataNotEquals(key string, value any) Condition {
	return DataCondition(key, OpNotEqual, value)
}

func MetaNotEquals(key string, value any) Condition {
	return MetaCondition(key, OpNotEqual, value)
}

func DataIsNotNull(key string) Condition {
	return DataCondition(key, OpIsNotNull, nil)
}

func MetaIsNotNull(key string) Condition {
	return MetaCondition(key, OpIsNotNull, nil)
}

func DataIsNull(key string) Condition {
	return DataCondition(key, OpIsNull, nil)
}

func MetaIsNull(key string) Condition {
	return MetaCondition(key, OpIsNull, nil)
}

func ColumnEquals(column string, value any) Condition {
	return NewCondition(column, OpEqual, value)
}

func StatusEquals(status string) Condition {
	return MetaEquals("status", status)
}

func DeletedAtIsNull() Condition {
	return NewCondition("deleted_at", OpIsNull, nil)
}

func IDEquals(id any) Condition {
	return ColumnEquals("id", id)
}

func (c Condition) getExpression() psql.Expression {
	if len(c.Keys) == 0 {
		return psql.Quote(c.Field)
	}
	var b strings.Builder
	fmt.Fprintf(&b, `"%s"`, c.Field)
	for i, key := range c.Keys {
		if i == len(c.Keys)-1 && c.JsonText {
			fmt.Fprintf(&b, `->>'%s'`, key)
		} else {
			fmt.Fprintf(&b, `->'%s'`, key)
		}
	}
	return psql.Raw(b.String())
}

type SelectQuery interface {
	Apply(mods ...bob.Mod[*dialect.SelectQuery])
}
type UpdateQuery interface {
	Apply(mods ...bob.Mod[*dialect.UpdateQuery])
}

func (c Condition) ApplyToSelect(query SelectQuery) {
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

func (c Condition) ApplyToUpdate(query UpdateQuery) {
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
