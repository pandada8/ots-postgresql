package plainbuffer_test

import (
	"bytes"
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"

	"github.com/pandada8/ots-postgresql/internal/ots"
	"github.com/pandada8/ots-postgresql/internal/plainbuffer"
)

func TestRoundtripAllTypes(t *testing.T) {
	row := plainbuffer.Row{
		PrimaryKey: []plainbuffer.Cell{
			{Name: "pk_str", Value: ots.StringValue("key-值")},
			{Name: "pk_int", Value: ots.IntegerValue(-9223372036854775808)},
			{Name: "pk_bin", Value: ots.BinaryValue([]byte{0x00, 0xff, 0x10})},
		},
		Cells: []plainbuffer.Cell{
			{Name: "s", Value: ots.StringValue("hello 世界")},
			{Name: "i", Value: ots.IntegerValue(1234567890123)},
			{Name: "d", Value: ots.DoubleValue(3.141592653589793)},
			{Name: "bt", Value: ots.BooleanValue(true)},
			{Name: "bf", Value: ots.BooleanValue(false)},
			{Name: "bin", Value: ots.BinaryValue([]byte{0xde, 0xad, 0xbe, 0xef})},
			{Name: "ts", Value: ots.StringValue("v"), HasTimestamp: true, Timestamp: 1700000000123},
			{Name: "empty", Value: ots.StringValue("")},
			{Name: "emptybin", Value: ots.BinaryValue([]byte{})},
		},
	}
	data := plainbuffer.EncodeRow(row)
	got, err := plainbuffer.DecodeRow(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got.PrimaryKey) != len(row.PrimaryKey) || len(got.Cells) != len(row.Cells) {
		t.Fatalf("cell count mismatch: pk=%d cells=%d", len(got.PrimaryKey), len(got.Cells))
	}
	for i, want := range row.PrimaryKey {
		assertCell(t, "pk", i, got.PrimaryKey[i], want)
	}
	for i, want := range row.Cells {
		assertCell(t, "cell", i, got.Cells[i], want)
	}
}

func assertCell(t *testing.T, kind string, idx int, got, want plainbuffer.Cell) {
	t.Helper()
	if got.Name != want.Name {
		t.Fatalf("%s[%d] name: got %q want %q", kind, idx, got.Name, want.Name)
	}
	if got.Special != want.Special {
		t.Fatalf("%s[%d] special: got %v want %v", kind, idx, got.Special, want.Special)
	}
	if !got.Value.Equal(want.Value) {
		t.Fatalf("%s[%d] value: got %v want %v", kind, idx, got.Value.GoString(), want.Value.GoString())
	}
	if got.HasTimestamp != want.HasTimestamp || got.Timestamp != want.Timestamp {
		t.Fatalf("%s[%d] timestamp: got (%v,%d) want (%v,%d)", kind, idx, got.HasTimestamp, got.Timestamp, want.HasTimestamp, want.Timestamp)
	}
	if got.HasOp != want.HasOp || got.Op != want.Op {
		t.Fatalf("%s[%d] op: got (%v,%d) want (%v,%d)", kind, idx, got.HasOp, got.Op, want.HasOp, want.Op)
	}
}

func TestRoundtripSpecialsAndOps(t *testing.T) {
	row := plainbuffer.Row{
		PrimaryKey: []plainbuffer.Cell{
			{Name: "pk", Special: ots.SpecialInfMin},
		},
		Cells: []plainbuffer.Cell{
			{Name: "del_all", Op: plainbuffer.OpDeleteAllVersion, HasOp: true},
			{Name: "del_one", Op: plainbuffer.OpDeleteOneVersion, HasOp: true, HasTimestamp: true, Timestamp: 1700000000000},
			{Name: "inc", Value: ots.IntegerValue(5), Op: plainbuffer.OpIncrement, HasOp: true},
			{Name: "max", Special: ots.SpecialInfMax},
			{Name: "auto", Special: ots.SpecialAutoIncrement},
		},
		DeleteMarker: true,
	}
	got, err := plainbuffer.DecodeRow(plainbuffer.EncodeRow(row))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !got.DeleteMarker {
		t.Fatalf("delete marker lost")
	}
	assertCell(t, "pk", 0, got.PrimaryKey[0], row.PrimaryKey[0])
	for i, want := range row.Cells {
		assertCell(t, "cell", i, got.Cells[i], want)
	}
}

func TestRoundtripMultipleRows(t *testing.T) {
	rows := []plainbuffer.Row{
		{PrimaryKey: []plainbuffer.Cell{{Name: "pk", Value: ots.StringValue("a")}}},
		{PrimaryKey: []plainbuffer.Cell{{Name: "pk", Value: ots.StringValue("b")}}, Cells: []plainbuffer.Cell{{Name: "c", Value: ots.IntegerValue(1)}}},
		{PrimaryKey: []plainbuffer.Cell{{Name: "pk", Value: ots.StringValue("c")}}, DeleteMarker: true},
	}
	got, err := plainbuffer.DecodeRows(plainbuffer.EncodeRows(rows))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("row count: got %d want %d", len(got), len(rows))
	}
	for i := range rows {
		if got[i].DeleteMarker != rows[i].DeleteMarker {
			t.Fatalf("row %d delete marker mismatch", i)
		}
		for j, want := range rows[i].PrimaryKey {
			assertCell(t, "pk", j, got[i].PrimaryKey[j], want)
		}
	}
}

func TestValueCodec(t *testing.T) {
	values := []*ots.Value{
		ots.IntegerValue(42),
		ots.IntegerValue(-1),
		ots.DoubleValue(2.5),
		ots.BooleanValue(true),
		ots.BooleanValue(false),
		ots.StringValue("héllo"),
		ots.StringValue(""),
		ots.BinaryValue([]byte{1, 2, 3}),
		ots.BinaryValue([]byte{}),
	}
	for _, want := range values {
		got, err := plainbuffer.DecodeValue(plainbuffer.EncodeValue(want))
		if err != nil {
			t.Fatalf("decode %v: %v", want.GoString(), err)
		}
		if !got.Equal(want) {
			t.Fatalf("value roundtrip: got %v want %v", got.GoString(), want.GoString())
		}
	}
}

func TestCrossValidatePutRow(t *testing.T) {
	pk := new(tablestore.PrimaryKey)
	pk.AddPrimaryKeyColumn("pk1", "key1")
	pk.AddPrimaryKeyColumn("pk2", int64(42))
	pk.AddPrimaryKeyColumn("pk3", []byte{0x01, 0x02})

	change := new(tablestore.PutRowChange)
	change.TableName = "t"
	change.PrimaryKey = pk
	change.AddColumn("s", "hello")
	change.AddColumn("i", int64(-7))
	change.AddColumn("d", float64(3.14))
	change.AddColumn("bt", true)
	change.AddColumn("bf", false)
	change.AddColumn("bin", []byte{0xff, 0x00})
	change.AddColumnWithTimestamp("ts", "v", 1700000000000)

	myRow := plainbuffer.Row{
		PrimaryKey: []plainbuffer.Cell{
			{Name: "pk1", Value: ots.StringValue("key1")},
			{Name: "pk2", Value: ots.IntegerValue(42)},
			{Name: "pk3", Value: ots.BinaryValue([]byte{0x01, 0x02})},
		},
		Cells: []plainbuffer.Cell{
			{Name: "s", Value: ots.StringValue("hello")},
			{Name: "i", Value: ots.IntegerValue(-7)},
			{Name: "d", Value: ots.DoubleValue(3.14)},
			{Name: "bt", Value: ots.BooleanValue(true)},
			{Name: "bf", Value: ots.BooleanValue(false)},
			{Name: "bin", Value: ots.BinaryValue([]byte{0xff, 0x00})},
			{Name: "ts", Value: ots.StringValue("v"), HasTimestamp: true, Timestamp: 1700000000000},
		},
	}
	assertBytesEqual(t, "PutRow", change.Serialize(), plainbuffer.EncodeRow(myRow))

	decoded, err := plainbuffer.DecodeRow(change.Serialize())
	if err != nil {
		t.Fatalf("decode SDK PutRow: %v", err)
	}
	if len(decoded.Cells) != 7 || !decoded.Cells[0].Value.Equal(ots.StringValue("hello")) {
		t.Fatalf("decoded SDK PutRow mismatch: %+v", decoded.Cells)
	}
}

func TestCrossValidatePrimaryKey(t *testing.T) {
	pk := new(tablestore.PrimaryKey)
	pk.AddPrimaryKeyColumn("pk1", "key1")
	pk.AddPrimaryKeyColumn("pk2", int64(7))
	pk.AddPrimaryKeyColumn("pk3", []byte{0xaa})

	myPK := []plainbuffer.Cell{
		{Name: "pk1", Value: ots.StringValue("key1")},
		{Name: "pk2", Value: ots.IntegerValue(7)},
		{Name: "pk3", Value: ots.BinaryValue([]byte{0xaa})},
	}
	assertBytesEqual(t, "PK", pk.Build(false), plainbuffer.EncodePrimaryKey(myPK, false))
	assertBytesEqual(t, "PK-delete", pk.Build(true), plainbuffer.EncodePrimaryKey(myPK, true))

	special := new(tablestore.PrimaryKey)
	special.AddPrimaryKeyColumnWithMinValue("pk1")
	special.AddPrimaryKeyColumnWithMaxValue("pk2")
	special.AddPrimaryKeyColumnWithAutoIncrement("pk3")
	mySpecial := []plainbuffer.Cell{
		{Name: "pk1", Special: ots.SpecialInfMin},
		{Name: "pk2", Special: ots.SpecialInfMax},
		{Name: "pk3", Special: ots.SpecialAutoIncrement},
	}
	assertBytesEqual(t, "PK-special", special.Build(false), plainbuffer.EncodePrimaryKey(mySpecial, false))
}

func TestCrossValidateUpdateRow(t *testing.T) {
	pk := new(tablestore.PrimaryKey)
	pk.AddPrimaryKeyColumn("pk1", "key1")

	change := new(tablestore.UpdateRowChange)
	change.TableName = "t"
	change.PrimaryKey = pk
	change.PutColumn("put", "value")
	change.PutColumnWithTimestamp("put_ts", int64(9), 1700000000000)
	change.DeleteColumn("del_all")
	change.DeleteColumnWithTimestamp("del_one", 1700000000000)
	change.IncrementColumn("inc", 3)

	myRow := plainbuffer.Row{
		PrimaryKey: []plainbuffer.Cell{{Name: "pk1", Value: ots.StringValue("key1")}},
		Cells: []plainbuffer.Cell{
			{Name: "put", Value: ots.StringValue("value")},
			{Name: "put_ts", Value: ots.IntegerValue(9), HasTimestamp: true, Timestamp: 1700000000000},
			{Name: "del_all", Op: plainbuffer.OpDeleteAllVersion, HasOp: true},
			{Name: "del_one", Op: plainbuffer.OpDeleteOneVersion, HasOp: true, HasTimestamp: true, Timestamp: 1700000000000},
			{Name: "inc", Value: ots.IntegerValue(3), Op: plainbuffer.OpIncrement, HasOp: true},
		},
	}
	assertBytesEqual(t, "UpdateRow", change.Serialize(), plainbuffer.EncodeRow(myRow))
}

func TestCrossValidateDeleteRow(t *testing.T) {
	pk := new(tablestore.PrimaryKey)
	pk.AddPrimaryKeyColumn("pk1", "key1")
	change := new(tablestore.DeleteRowChange)
	change.TableName = "t"
	change.PrimaryKey = pk
	myPK := []plainbuffer.Cell{{Name: "pk1", Value: ots.StringValue("key1")}}
	assertBytesEqual(t, "DeleteRow", change.Serialize(), plainbuffer.EncodePrimaryKey(myPK, true))
}

func assertBytesEqual(t *testing.T, label string, sdk, mine []byte) {
	t.Helper()
	if !bytes.Equal(sdk, mine) {
		t.Fatalf("%s byte mismatch:\n SDK (%d): %x\n mine(%d): %x", label, len(sdk), sdk, len(mine), mine)
	}
}

func TestDecodeRejectsCorruption(t *testing.T) {
	row := plainbuffer.Row{
		PrimaryKey: []plainbuffer.Cell{{Name: "pk", Value: ots.StringValue("a")}},
		Cells:      []plainbuffer.Cell{{Name: "c", Value: ots.IntegerValue(1)}},
	}
	data := plainbuffer.EncodeRow(row)
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)-1] ^= 0xff
	if _, err := plainbuffer.DecodeRow(corrupt); err == nil {
		t.Fatalf("expected checksum error for corrupted row")
	}
	if _, err := plainbuffer.DecodeRow([]byte{0x00, 0x00, 0x00, 0x00}); err == nil {
		t.Fatalf("expected header error")
	}
}
