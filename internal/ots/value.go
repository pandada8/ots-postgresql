package ots

import (
	"bytes"
	"fmt"
	"math"
)

type ValueType int

const (
	ValueInteger ValueType = iota
	ValueDouble
	ValueBoolean
	ValueString
	ValueBinary
)

func (t ValueType) String() string {
	switch t {
	case ValueInteger:
		return "INTEGER"
	case ValueDouble:
		return "DOUBLE"
	case ValueBoolean:
		return "BOOLEAN"
	case ValueString:
		return "STRING"
	case ValueBinary:
		return "BINARY"
	default:
		return "UNKNOWN"
	}
}

type Value struct {
	Type    ValueType
	Integer int64
	Double  float64
	Boolean bool
	String  string
	Binary  []byte
}

func IntegerValue(v int64) *Value  { return &Value{Type: ValueInteger, Integer: v} }
func DoubleValue(v float64) *Value { return &Value{Type: ValueDouble, Double: v} }
func BooleanValue(v bool) *Value   { return &Value{Type: ValueBoolean, Boolean: v} }
func StringValue(v string) *Value  { return &Value{Type: ValueString, String: v} }
func BinaryValue(v []byte) *Value  { return &Value{Type: ValueBinary, Binary: v} }

func (v *Value) Equal(other *Value) bool {
	if v == nil || other == nil {
		return v == other
	}
	if v.Type != other.Type {
		return false
	}
	switch v.Type {
	case ValueInteger:
		return v.Integer == other.Integer
	case ValueDouble:
		return v.Double == other.Double
	case ValueBoolean:
		return v.Boolean == other.Boolean
	case ValueString:
		return v.String == other.String
	case ValueBinary:
		return bytes.Equal(v.Binary, other.Binary)
	}
	return false
}

func (v *Value) Compare(other *Value) (int, error) {
	if v.Type != other.Type {
		return 0, fmt.Errorf("cannot compare %s with %s", v.Type, other.Type)
	}
	switch v.Type {
	case ValueInteger:
		switch {
		case v.Integer < other.Integer:
			return -1, nil
		case v.Integer > other.Integer:
			return 1, nil
		}
		return 0, nil
	case ValueDouble:
		switch {
		case v.Double < other.Double:
			return -1, nil
		case v.Double > other.Double:
			return 1, nil
		}
		return 0, nil
	case ValueBoolean:
		switch {
		case !v.Boolean && other.Boolean:
			return -1, nil
		case v.Boolean && !other.Boolean:
			return 1, nil
		}
		return 0, nil
	case ValueString:
		return bytes.Compare([]byte(v.String), []byte(other.String)), nil
	case ValueBinary:
		return bytes.Compare(v.Binary, other.Binary), nil
	}
	return 0, fmt.Errorf("unsupported value type %s", v.Type)
}

func (v *Value) GoString() string {
	switch v.Type {
	case ValueInteger:
		return fmt.Sprintf("%d", v.Integer)
	case ValueDouble:
		return fmt.Sprintf("%g", v.Double)
	case ValueBoolean:
		return fmt.Sprintf("%t", v.Boolean)
	case ValueString:
		return fmt.Sprintf("%q", v.String)
	case ValueBinary:
		return fmt.Sprintf("0x%x", v.Binary)
	}
	return "<nil>"
}

func (v *Value) Float64() float64 {
	switch v.Type {
	case ValueInteger:
		return float64(v.Integer)
	case ValueDouble:
		return v.Double
	}
	return math.NaN()
}

type PrimaryKeyType int

const (
	PrimaryKeyInteger PrimaryKeyType = iota + 1
	PrimaryKeyString
	PrimaryKeyBinary
)

func (t PrimaryKeyType) String() string {
	switch t {
	case PrimaryKeyInteger:
		return "INTEGER"
	case PrimaryKeyString:
		return "STRING"
	case PrimaryKeyBinary:
		return "BINARY"
	}
	return "UNKNOWN"
}

type SpecialKey int

const (
	SpecialNone SpecialKey = iota
	SpecialInfMin
	SpecialInfMax
	SpecialAutoIncrement
)

type PrimaryKeyColumn struct {
	Name    string
	Type    PrimaryKeyType
	Value   *Value
	Special SpecialKey
}

type PrimaryKey []PrimaryKeyColumn

func (pk PrimaryKey) Clone() PrimaryKey {
	out := make(PrimaryKey, len(pk))
	copy(out, pk)
	return out
}

type AttributeColumn struct {
	Name      string
	Value     *Value
	Timestamp int64
}

type Row struct {
	PrimaryKey PrimaryKey
	Attributes []AttributeColumn
}
