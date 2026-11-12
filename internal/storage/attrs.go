package storage

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/pandada8/ots-postgresql/internal/ots"
)

const (
	attrTypeInteger = "I"
	attrTypeDouble  = "D"
	attrTypeBoolean = "B"
	attrTypeString  = "S"
	attrTypeBinary  = "X"
)

type attrVersion struct {
	Ts  int64       `json:"ts"`
	Val interface{} `json:"val"`
}

type attrColumn struct {
	Type     string        `json:"t"`
	Versions []attrVersion `json:"v"`
}

type attrs map[string]*attrColumn

func attrTypeOf(v *ots.Value) string {
	switch v.Type {
	case ots.ValueInteger:
		return attrTypeInteger
	case ots.ValueDouble:
		return attrTypeDouble
	case ots.ValueBoolean:
		return attrTypeBoolean
	case ots.ValueString:
		return attrTypeString
	case ots.ValueBinary:
		return attrTypeBinary
	}
	return ""
}

func encodeAttrValue(v *ots.Value) interface{} {
	switch v.Type {
	case ots.ValueInteger:
		return v.Integer
	case ots.ValueDouble:
		return v.Double
	case ots.ValueBoolean:
		return v.Boolean
	case ots.ValueString:
		return v.String
	case ots.ValueBinary:
		return base64.StdEncoding.EncodeToString(v.Binary)
	}
	return nil
}

func decodeAttrValue(colType string, raw interface{}) (*ots.Value, error) {
	switch colType {
	case attrTypeInteger:
		v, err := rawInt64(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid integer attribute value %v", raw)
		}
		return ots.IntegerValue(v), nil
	case attrTypeDouble:
		v, err := rawFloat64(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid double attribute value %v", raw)
		}
		return ots.DoubleValue(v), nil
	case attrTypeBoolean:
		b, ok := raw.(bool)
		if !ok {
			return nil, fmt.Errorf("invalid boolean attribute value %v", raw)
		}
		return ots.BooleanValue(b), nil
	case attrTypeString:
		s, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("invalid string attribute value %v", raw)
		}
		return ots.StringValue(s), nil
	case attrTypeBinary:
		s, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("invalid binary attribute value %v", raw)
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("invalid binary attribute value: %v", err)
		}
		return ots.BinaryValue(b), nil
	}
	return nil, fmt.Errorf("unknown attribute type %q", colType)
}

func rawInt64(raw interface{}) (int64, error) {
	switch v := raw.(type) {
	case json.Number:
		return v.Int64()
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	}
	return 0, fmt.Errorf("not an integer")
}

func rawFloat64(raw interface{}) (float64, error) {
	switch v := raw.(type) {
	case json.Number:
		return v.Float64()
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	}
	return 0, fmt.Errorf("not a number")
}

func decodeAttrs(data []byte) (attrs, error) {
	result := make(attrs)
	if len(bytes.TrimSpace(data)) == 0 {
		return result, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, fmt.Errorf("decode attributes: %w", err)
	}
	for name, col := range result {
		if col == nil {
			return nil, fmt.Errorf("null attribute column %q", name)
		}
		sort.SliceStable(col.Versions, func(i, j int) bool {
			return col.Versions[i].Ts > col.Versions[j].Ts
		})
		_ = name
	}
	return result, nil
}

func encodeAttrs(a attrs) ([]byte, error) {
	if a == nil {
		a = make(attrs)
	}
	return json.Marshal(a)
}

func (a attrs) toVersions() (map[string][]ots.AttributeColumn, error) {
	result := make(map[string][]ots.AttributeColumn, len(a))
	for name, col := range a {
		versions := make([]ots.AttributeColumn, 0, len(col.Versions))
		for _, version := range col.Versions {
			value, err := decodeAttrValue(col.Type, version.Val)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", name, err)
			}
			versions = append(versions, ots.AttributeColumn{Name: name, Value: value, Timestamp: version.Ts})
		}
		result[name] = versions
	}
	return result, nil
}

func trimVersions(col *attrColumn, maxVersions int32) {
	if maxVersions > 0 && int32(len(col.Versions)) > maxVersions {
		col.Versions = col.Versions[:maxVersions]
	}
}

func (a attrs) put(name string, value *ots.Value, ts int64, maxVersions int32) error {
	colType := attrTypeOf(value)
	col, ok := a[name]
	if !ok {
		col = &attrColumn{Type: colType}
		a[name] = col
	} else if col.Type != colType {
		return ots.ErrParameterInvalid("attribute column %q type mismatch: existing %s, got %s", name, col.Type, colType)
	}
	encoded := encodeAttrValue(value)
	replaced := false
	for i := range col.Versions {
		if col.Versions[i].Ts == ts {
			col.Versions[i].Val = encoded
			replaced = true
			break
		}
	}
	if !replaced {
		col.Versions = append(col.Versions, attrVersion{Ts: ts, Val: encoded})
		sort.SliceStable(col.Versions, func(i, j int) bool {
			return col.Versions[i].Ts > col.Versions[j].Ts
		})
	}
	trimVersions(col, maxVersions)
	return nil
}

func (a attrs) deleteAll(name string) {
	delete(a, name)
}

func (a attrs) deleteOne(name string, ts int64) {
	col, ok := a[name]
	if !ok {
		return
	}
	for i := range col.Versions {
		if col.Versions[i].Ts == ts {
			col.Versions = append(col.Versions[:i], col.Versions[i+1:]...)
			break
		}
	}
	if len(col.Versions) == 0 {
		delete(a, name)
	}
}

func (a attrs) increment(name string, delta int64, ts int64, maxVersions int32) (*ots.Value, error) {
	col, ok := a[name]
	if !ok || len(col.Versions) == 0 {
		return nil, ots.ErrParameterInvalid("increment column %q does not exist", name)
	}
	if col.Type != attrTypeInteger {
		return nil, ots.ErrParameterInvalid("increment column %q is not INTEGER", name)
	}
	latest, err := decodeAttrValue(col.Type, col.Versions[0].Val)
	if err != nil {
		return nil, err
	}
	result := ots.IntegerValue(latest.Integer + delta)
	if err := a.put(name, result, ts, maxVersions); err != nil {
		return nil, err
	}
	return result, nil
}
