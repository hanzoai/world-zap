package proto

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		typ     byte
		payload any
	}{
		{"init", TypeINIT, map[string]any{"version": 1, "protocol": "zap"}},
		{"ping", TypePING, nil},
		{"list_tools", TypeListTools, map[string]any{}},
		{"call_tool_subscribe", TypeCallTool, map[string]any{
			"name": "subscribe",
			"args": map[string]any{"topic": "world.events.earthquakes"},
		}},
		{"push", TypePUSH, map[string]any{
			"topic": "world.events.earthquakes",
			"event": map[string]any{"id": "usgs:42", "mag": 5.6},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := EncodeJSON(tc.typ, tc.payload)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if len(b) < 9 {
				t.Fatalf("frame too small: %d", len(b))
			}
			f, n, err := Decode(b)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if n != len(b) {
				t.Fatalf("consumed %d want %d", n, len(b))
			}
			if f.Type != tc.typ {
				t.Fatalf("type got 0x%02X want 0x%02X", f.Type, tc.typ)
			}
			if tc.payload == nil {
				if len(f.Payload) != 0 {
					t.Fatalf("expected empty payload, got %d bytes", len(f.Payload))
				}
				return
			}
			want, _ := json.Marshal(tc.payload)
			if !bytes.Equal(f.Payload, want) {
				t.Fatalf("payload mismatch:\n got %s\nwant %s", f.Payload, want)
			}
		})
	}
}

func TestDecodeRejectsBadMagic(t *testing.T) {
	b := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0, 0, 0, 0}
	if _, _, err := Decode(b); err != ErrBadMagic {
		t.Fatalf("want ErrBadMagic got %v", err)
	}
}

func TestDecodeRejectsOversize(t *testing.T) {
	b := make([]byte, 9)
	copy(b, Magic[:])
	b[4] = TypePUSH
	// length bigger than MaxPayload
	b[5] = 0xFF
	b[6] = 0xFF
	b[7] = 0xFF
	b[8] = 0xFF
	if _, _, err := Decode(b); err != ErrPayloadTooLarge {
		t.Fatalf("want ErrPayloadTooLarge got %v", err)
	}
}

func TestTypeName(t *testing.T) {
	got := TypeName(TypePUSH)
	if got != "PUSH" {
		t.Fatalf("got %q", got)
	}
	if TypeName(0x99) != "0x99" {
		t.Fatalf("unknown type name wrong")
	}
}
