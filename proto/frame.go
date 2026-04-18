// Package proto implements the ZAP wire format used by world-zap.
//
// Frame layout (big-endian):
//
//	 0         1         2         3         4                   9 ... 9+N
//	+---------+---------+---------+---------+---------+----+----+--------+
//	|   0x5A  |  0x41   |  0x50   |  0x01   |  TYPE   | LENGTH  | PAYLOAD|
//	+---------+---------+---------+---------+---------+---------+--------+
//	    magic "ZAP" + version byte 0x01            4-byte BE    JSON bytes
//
// The 9-byte header is: magic(4) + type(1) + length(4, payload bytes).
// Payload is an opaque JSON object; its schema is type-dependent.
package proto

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// Magic bytes + version identifier "ZAP\x01".
var Magic = [4]byte{0x5A, 0x41, 0x50, 0x01}

// Frame types. Values align with the task spec.
const (
	TypeINIT      = 0x01
	TypeINITACK   = 0x02
	TypePUSH      = 0x10
	TypeRESOLVE   = 0x12
	TypeListTools = 0x20
	TypeCallTool  = 0x22
	TypePING      = 0xF0
	TypePONG      = 0xF1
	TypeERROR     = 0xFE
)

// MaxPayload caps individual JSON payload size. 4 MiB is plenty for pushes
// but prevents a hostile client from exhausting memory.
const MaxPayload = 4 * 1024 * 1024

// Frame is a decoded ZAP message.
type Frame struct {
	Type    byte
	Payload []byte // raw JSON
}

// ErrBadMagic is returned when the 4-byte magic prefix is wrong.
var ErrBadMagic = errors.New("zap: bad magic")

// ErrPayloadTooLarge is returned when length exceeds MaxPayload.
var ErrPayloadTooLarge = errors.New("zap: payload too large")

// Encode serializes a frame to a single byte slice suitable for
// websocket.BinaryMessage. Payload is copied as-is; the caller must ensure
// it is valid JSON for the chosen type.
func Encode(f Frame) ([]byte, error) {
	if len(f.Payload) > MaxPayload {
		return nil, ErrPayloadTooLarge
	}
	buf := make([]byte, 9+len(f.Payload))
	copy(buf[0:4], Magic[:])
	buf[4] = f.Type
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(f.Payload)))
	copy(buf[9:], f.Payload)
	return buf, nil
}

// EncodeJSON marshals v to JSON and wraps it in a frame.
func EncodeJSON(t byte, v any) ([]byte, error) {
	if v == nil {
		return Encode(Frame{Type: t})
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("zap: marshal: %w", err)
	}
	return Encode(Frame{Type: t, Payload: data})
}

// Decode parses a single frame out of b. Returns the frame and the number of
// bytes consumed. If b is too short for a complete frame returns io.ErrUnexpectedEOF.
func Decode(b []byte) (Frame, int, error) {
	if len(b) < 9 {
		return Frame{}, 0, io.ErrUnexpectedEOF
	}
	if b[0] != Magic[0] || b[1] != Magic[1] || b[2] != Magic[2] || b[3] != Magic[3] {
		return Frame{}, 0, ErrBadMagic
	}
	typ := b[4]
	n := binary.BigEndian.Uint32(b[5:9])
	if n > MaxPayload {
		return Frame{}, 0, ErrPayloadTooLarge
	}
	if len(b) < 9+int(n) {
		return Frame{}, 0, io.ErrUnexpectedEOF
	}
	// Copy so the caller can retain the payload past the buffer's lifetime.
	payload := make([]byte, n)
	copy(payload, b[9:9+n])
	return Frame{Type: typ, Payload: payload}, 9 + int(n), nil
}

// TypeName returns a human-readable type label for logs and errors.
func TypeName(t byte) string {
	switch t {
	case TypeINIT:
		return "INIT"
	case TypeINITACK:
		return "INIT_ACK"
	case TypePUSH:
		return "PUSH"
	case TypeRESOLVE:
		return "RESOLVE"
	case TypeListTools:
		return "LIST_TOOLS"
	case TypeCallTool:
		return "CALL_TOOL"
	case TypePING:
		return "PING"
	case TypePONG:
		return "PONG"
	case TypeERROR:
		return "ERROR"
	default:
		return fmt.Sprintf("0x%02X", t)
	}
}
