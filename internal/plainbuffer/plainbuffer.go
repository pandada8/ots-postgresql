// Package plainbuffer implements the OTS PlainBuffer row encoding.
package plainbuffer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/pandada8/ots-postgresql/internal/ots"
)

const (
	header = 0x75

	tagRowPK           = 0x1
	tagRowData         = 0x2
	tagCell            = 0x3
	tagCellName        = 0x4
	tagCellValue       = 0x5
	tagCellType        = 0x6
	tagCellTimestamp   = 0x7
	tagDeleteRowMarker = 0x8
	tagRowChecksum     = 0x9
	tagCellChecksum    = 0x0A
	tagExtension       = 0x0B
	tagSeqInfo         = 0x0C
	tagSeqInfoEpoch    = 0x0D
	tagSeqInfoTs       = 0x0E
	tagSeqInfoRowIndex = 0x0F

	vtInteger       = 0x0
	vtDouble        = 0x1
	vtBoolean       = 0x2
	vtString        = 0x3
	vtNull          = 0x6
	vtBlob          = 0x7
	vtInfMin        = 0x9
	vtInfMax        = 0xa
	vtAutoIncrement = 0xb
)

type CellOp byte

const (
	OpPut              CellOp = 0x0
	OpDeleteAllVersion CellOp = 0x1
	OpDeleteOneVersion CellOp = 0x3
	OpIncrement        CellOp = 0x4
)

type Cell struct {
	Name         string
	Value        *ots.Value
	Special      ots.SpecialKey
	Timestamp    int64
	HasTimestamp bool
	Op           CellOp
	HasOp        bool
}

type Row struct {
	PrimaryKey   []Cell
	Cells        []Cell
	DeleteMarker bool
}

var crc8Table [256]byte

func init() {
	for i := 0; i < 256; i++ {
		x := byte(i)
		for j := 0; j < 8; j++ {
			if x&0x80 != 0 {
				x = (x << 1) ^ 0x07
			} else {
				x <<= 1
			}
		}
		crc8Table[i] = x
	}
}

func crc8Byte(crc, in byte) byte {
	return crc8Table[crc^in]
}

func crc8Bytes(crc byte, in []byte) byte {
	for _, b := range in {
		crc = crc8Byte(crc, b)
	}
	return crc
}

func crc8Int32(crc byte, in int32) byte {
	for i := 0; i < 4; i++ {
		crc = crc8Byte(crc, byte(in))
		in >>= 8
	}
	return crc
}

func crc8Int64(crc byte, in int64) byte {
	for i := 0; i < 8; i++ {
		crc = crc8Byte(crc, byte(in))
		in >>= 8
	}
	return crc
}

type writer struct {
	buf bytes.Buffer
}

func (w *writer) byte(b byte) {
	w.buf.WriteByte(b)
}

func (w *writer) le32(v int32) {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(v))
	w.buf.Write(tmp[:])
}

func (w *writer) le64(v int64) {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], uint64(v))
	w.buf.Write(tmp[:])
}

func (w *writer) raw(p []byte) {
	w.buf.Write(p)
}

func specialTypeByte(s ots.SpecialKey) (byte, bool) {
	switch s {
	case ots.SpecialInfMin:
		return vtInfMin, true
	case ots.SpecialInfMax:
		return vtInfMax, true
	case ots.SpecialAutoIncrement:
		return vtAutoIncrement, true
	}
	return 0, false
}

func (w *writer) cellValue(v *ots.Value, special ots.SpecialKey) {
	w.byte(tagCellValue)
	if tb, ok := specialTypeByte(special); ok {
		w.le32(1)
		w.byte(tb)
		return
	}
	switch v.Type {
	case ots.ValueInteger:
		w.le32(9)
		w.byte(vtInteger)
		w.le64(v.Integer)
	case ots.ValueDouble:
		w.le32(9)
		w.byte(vtDouble)
		w.le64(int64(math.Float64bits(v.Double)))
	case ots.ValueBoolean:
		w.le32(2)
		w.byte(vtBoolean)
		if v.Boolean {
			w.byte(1)
		} else {
			w.byte(0)
		}
	case ots.ValueString:
		w.le32(int32(5 + len(v.String)))
		w.byte(vtString)
		w.le32(int32(len(v.String)))
		w.raw([]byte(v.String))
	case ots.ValueBinary:
		w.le32(int32(5 + len(v.Binary)))
		w.byte(vtBlob)
		w.le32(int32(len(v.Binary)))
		w.raw(v.Binary)
	}
}

func valueChecksum(crc byte, v *ots.Value, special ots.SpecialKey) byte {
	if tb, ok := specialTypeByte(special); ok {
		return crc8Byte(crc, tb)
	}
	switch v.Type {
	case ots.ValueInteger:
		crc = crc8Byte(crc, vtInteger)
		crc = crc8Int64(crc, v.Integer)
	case ots.ValueDouble:
		crc = crc8Byte(crc, vtDouble)
		crc = crc8Int64(crc, int64(math.Float64bits(v.Double)))
	case ots.ValueBoolean:
		crc = crc8Byte(crc, vtBoolean)
		if v.Boolean {
			crc = crc8Byte(crc, 1)
		} else {
			crc = crc8Byte(crc, 0)
		}
	case ots.ValueString:
		crc = crc8Byte(crc, vtString)
		crc = crc8Int32(crc, int32(len(v.String)))
		crc = crc8Bytes(crc, []byte(v.String))
	case ots.ValueBinary:
		crc = crc8Byte(crc, vtBlob)
		crc = crc8Int32(crc, int32(len(v.Binary)))
		crc = crc8Bytes(crc, v.Binary)
	}
	return crc
}

func (c *Cell) hasValue() bool {
	return c.Value != nil || c.Special != ots.SpecialNone
}

func (c *Cell) checksum() byte {
	crc := crc8Bytes(0, []byte(c.Name))
	if c.hasValue() {
		crc = valueChecksum(crc, c.Value, c.Special)
	}
	if c.HasTimestamp {
		crc = crc8Int64(crc, c.Timestamp)
	}
	if c.HasOp {
		crc = crc8Byte(crc, byte(c.Op))
	}
	return crc
}

func (w *writer) cell(c *Cell) {
	w.byte(tagCell)
	w.byte(tagCellName)
	w.le32(int32(len(c.Name)))
	w.raw([]byte(c.Name))
	if c.hasValue() {
		w.cellValue(c.Value, c.Special)
	}
	if c.HasOp {
		w.byte(tagCellType)
		w.byte(byte(c.Op))
	}
	if c.HasTimestamp {
		w.byte(tagCellTimestamp)
		w.le64(c.Timestamp)
	}
	w.byte(tagCellChecksum)
	w.byte(c.checksum())
}

func (w *writer) row(r *Row) {
	w.byte(tagRowPK)
	for i := range r.PrimaryKey {
		w.cell(&r.PrimaryKey[i])
	}
	if len(r.Cells) > 0 {
		w.byte(tagRowData)
		for i := range r.Cells {
			w.cell(&r.Cells[i])
		}
	}
	if r.DeleteMarker {
		w.byte(tagDeleteRowMarker)
	}
	w.byte(tagRowChecksum)
	w.byte(rowChecksum(r))
}

func rowChecksum(r *Row) byte {
	var crc byte
	for i := range r.PrimaryKey {
		crc = crc8Byte(crc, r.PrimaryKey[i].checksum())
	}
	for i := range r.Cells {
		crc = crc8Byte(crc, r.Cells[i].checksum())
	}
	if r.DeleteMarker {
		crc = crc8Byte(crc, 1)
	} else {
		crc = crc8Byte(crc, 0)
	}
	return crc
}

// EncodeRows encodes rows with the PlainBuffer header.
func EncodeRows(rows []Row) []byte {
	w := &writer{}
	w.le32(header)
	for i := range rows {
		w.row(&rows[i])
	}
	return w.buf.Bytes()
}

// EncodeRow encodes a single row with the PlainBuffer header.
func EncodeRow(r Row) []byte {
	return EncodeRows([]Row{r})
}

// EncodePrimaryKey encodes a primary-key-only row.
func EncodePrimaryKey(pk []Cell, deleteMarker bool) []byte {
	return EncodeRow(Row{PrimaryKey: pk, DeleteMarker: deleteMarker})
}

// EncodeValue encodes a value without the length prefix (SQLVariant form
// used inside filters).
func EncodeValue(v *ots.Value) []byte {
	w := &writer{}
	switch v.Type {
	case ots.ValueInteger:
		w.byte(vtInteger)
		w.le64(v.Integer)
	case ots.ValueDouble:
		w.byte(vtDouble)
		w.le64(int64(math.Float64bits(v.Double)))
	case ots.ValueBoolean:
		w.byte(vtBoolean)
		if v.Boolean {
			w.byte(1)
		} else {
			w.byte(0)
		}
	case ots.ValueString:
		w.byte(vtString)
		w.le32(int32(len(v.String)))
		w.raw([]byte(v.String))
	case ots.ValueBinary:
		w.byte(vtBlob)
		w.le32(int32(len(v.Binary)))
		w.raw(v.Binary)
	}
	return w.buf.Bytes()
}

type reader struct {
	buf []byte
	pos int
}

func (r *reader) remaining() int {
	return len(r.buf) - r.pos
}

func (r *reader) byte() (byte, error) {
	if r.remaining() < 1 {
		return 0, fmt.Errorf("plainbuffer: unexpected end of input")
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *reader) le32() (int32, error) {
	if r.remaining() < 4 {
		return 0, fmt.Errorf("plainbuffer: unexpected end of input")
	}
	v := int32(binary.LittleEndian.Uint32(r.buf[r.pos:]))
	r.pos += 4
	return v, nil
}

func (r *reader) le64() (int64, error) {
	if r.remaining() < 8 {
		return 0, fmt.Errorf("plainbuffer: unexpected end of input")
	}
	v := int64(binary.LittleEndian.Uint64(r.buf[r.pos:]))
	r.pos += 8
	return v, nil
}

func (r *reader) raw(n int) ([]byte, error) {
	if n < 0 || r.remaining() < n {
		return nil, fmt.Errorf("plainbuffer: unexpected end of input")
	}
	v := r.buf[r.pos : r.pos+n]
	r.pos += n
	return v, nil
}

// DecodeValue decodes a SQLVariant value (no length prefix).
func DecodeValue(data []byte) (*ots.Value, error) {
	r := &reader{buf: data}
	v, err := r.value()
	if err != nil {
		return nil, err
	}
	if r.remaining() != 0 {
		return nil, fmt.Errorf("plainbuffer: trailing bytes in value")
	}
	return v, nil
}

func (r *reader) value() (*ots.Value, error) {
	tb, err := r.byte()
	if err != nil {
		return nil, err
	}
	switch tb {
	case vtInteger:
		v, err := r.le64()
		if err != nil {
			return nil, err
		}
		return ots.IntegerValue(v), nil
	case vtDouble:
		v, err := r.le64()
		if err != nil {
			return nil, err
		}
		return ots.DoubleValue(math.Float64frombits(uint64(v))), nil
	case vtBoolean:
		v, err := r.byte()
		if err != nil {
			return nil, err
		}
		return ots.BooleanValue(v != 0), nil
	case vtString:
		n, err := r.le32()
		if err != nil {
			return nil, err
		}
		s, err := r.raw(int(n))
		if err != nil {
			return nil, err
		}
		return ots.StringValue(string(s)), nil
	case vtBlob:
		n, err := r.le32()
		if err != nil {
			return nil, err
		}
		b, err := r.raw(int(n))
		if err != nil {
			return nil, err
		}
		return ots.BinaryValue(append([]byte(nil), b...)), nil
	default:
		return nil, fmt.Errorf("plainbuffer: invalid value type 0x%x", tb)
	}
}

func (r *reader) cellValue() (*ots.Value, ots.SpecialKey, error) {
	size, err := r.le32()
	if err != nil {
		return nil, ots.SpecialNone, err
	}
	tb, err := r.byte()
	if err != nil {
		return nil, ots.SpecialNone, err
	}
	switch tb {
	case vtInfMin:
		return nil, ots.SpecialInfMin, nil
	case vtInfMax:
		return nil, ots.SpecialInfMax, nil
	case vtAutoIncrement:
		return nil, ots.SpecialAutoIncrement, nil
	}
	start := r.pos
	var v *ots.Value
	switch tb {
	case vtInteger:
		n, err := r.le64()
		if err != nil {
			return nil, ots.SpecialNone, err
		}
		v = ots.IntegerValue(n)
	case vtDouble:
		n, err := r.le64()
		if err != nil {
			return nil, ots.SpecialNone, err
		}
		v = ots.DoubleValue(math.Float64frombits(uint64(n)))
	case vtBoolean:
		b, err := r.byte()
		if err != nil {
			return nil, ots.SpecialNone, err
		}
		v = ots.BooleanValue(b != 0)
	case vtString:
		n, err := r.le32()
		if err != nil {
			return nil, ots.SpecialNone, err
		}
		s, err := r.raw(int(n))
		if err != nil {
			return nil, ots.SpecialNone, err
		}
		v = ots.StringValue(string(s))
	case vtBlob:
		n, err := r.le32()
		if err != nil {
			return nil, ots.SpecialNone, err
		}
		b, err := r.raw(int(n))
		if err != nil {
			return nil, ots.SpecialNone, err
		}
		v = ots.BinaryValue(append([]byte(nil), b...))
	default:
		return nil, ots.SpecialNone, fmt.Errorf("plainbuffer: invalid cell value type 0x%x", tb)
	}
	if int(size) != 1+(r.pos-start) {
		return nil, ots.SpecialNone, fmt.Errorf("plainbuffer: cell value size mismatch")
	}
	return v, ots.SpecialNone, nil
}

func (r *reader) cell() (Cell, error) {
	var c Cell
	tag, err := r.byte()
	if err != nil {
		return c, err
	}
	if tag != tagCellName {
		return c, fmt.Errorf("plainbuffer: expected cell name tag, got 0x%x", tag)
	}
	n, err := r.le32()
	if err != nil {
		return c, err
	}
	name, err := r.raw(int(n))
	if err != nil {
		return c, err
	}
	c.Name = string(name)
	tag, err = r.byte()
	if err != nil {
		return c, err
	}
	if tag == tagCellValue {
		c.Value, c.Special, err = r.cellValue()
		if err != nil {
			return c, err
		}
		tag, err = r.byte()
		if err != nil {
			return c, err
		}
	}
	if tag == tagCellType {
		op, err := r.byte()
		if err != nil {
			return c, err
		}
		c.Op = CellOp(op)
		c.HasOp = true
		tag, err = r.byte()
		if err != nil {
			return c, err
		}
	}
	if tag == tagCellTimestamp {
		c.Timestamp, err = r.le64()
		if err != nil {
			return c, err
		}
		c.HasTimestamp = true
		tag, err = r.byte()
		if err != nil {
			return c, err
		}
	}
	if tag != tagCellChecksum {
		return c, fmt.Errorf("plainbuffer: expected cell checksum tag, got 0x%x", tag)
	}
	sum, err := r.byte()
	if err != nil {
		return c, err
	}
	if sum != c.checksum() {
		return c, fmt.Errorf("plainbuffer: cell checksum mismatch for %q", c.Name)
	}
	return c, nil
}

func (r *reader) cells() ([]Cell, error) {
	var cells []Cell
	for {
		tag, err := r.byte()
		if err != nil {
			return nil, err
		}
		if tag != tagCell {
			r.pos--
			return cells, nil
		}
		c, err := r.cell()
		if err != nil {
			return nil, err
		}
		cells = append(cells, c)
	}
}

func (r *reader) row() (Row, error) {
	var row Row
	tag, err := r.byte()
	if err != nil {
		return row, err
	}
	if tag == tagRowPK {
		row.PrimaryKey, err = r.cells()
		if err != nil {
			return row, err
		}
		tag, err = r.byte()
		if err != nil {
			return row, err
		}
	}
	if tag == tagRowData {
		row.Cells, err = r.cells()
		if err != nil {
			return row, err
		}
		tag, err = r.byte()
		if err != nil {
			return row, err
		}
	}
	if tag == tagDeleteRowMarker {
		row.DeleteMarker = true
		tag, err = r.byte()
		if err != nil {
			return row, err
		}
	}
	if tag == tagExtension {
		n, err := r.le32()
		if err != nil {
			return row, err
		}
		if _, err := r.raw(int(n)); err != nil {
			return row, err
		}
		tag, err = r.byte()
		if err != nil {
			return row, err
		}
	}
	if tag != tagRowChecksum {
		return row, fmt.Errorf("plainbuffer: expected row checksum tag, got 0x%x", tag)
	}
	sum, err := r.byte()
	if err != nil {
		return row, err
	}
	if sum != rowChecksum(&row) {
		return row, fmt.Errorf("plainbuffer: row checksum mismatch")
	}
	return row, nil
}

// DecodeRows decodes a header-prefixed PlainBuffer block containing rows.
func DecodeRows(data []byte) ([]Row, error) {
	r := &reader{buf: data}
	h, err := r.le32()
	if err != nil {
		return nil, err
	}
	if h != header {
		return nil, fmt.Errorf("plainbuffer: invalid header 0x%x", h)
	}
	var rows []Row
	for r.remaining() > 0 {
		row, err := r.row()
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// DecodeRow decodes a header-prefixed block containing exactly one row.
func DecodeRow(data []byte) (Row, error) {
	rows, err := DecodeRows(data)
	if err != nil {
		return Row{}, err
	}
	if len(rows) != 1 {
		return Row{}, fmt.Errorf("plainbuffer: expected 1 row, got %d", len(rows))
	}
	return rows[0], nil
}
