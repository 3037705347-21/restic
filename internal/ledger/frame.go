package ledger

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/restic/restic/internal/restic"
)

// Frame layout (little-endian length field, CRC32-IEEE over header+payload):
//
//	magic   [4]byte "RSTL"
//	version uint8
//	op      uint8
//	length  uint32   (length of the JSON payload)
//	payload []byte   (JSON encoded wireFrame)
//	crc     uint32   (checksum over all preceding bytes of the frame)
const (
	frameMagic   = "RSTL"
	frameHeader  = 4 + 1 + 1 + 4
	frameTrailer = 4
	// maxFramePayload bounds a single record frame. Salvage lists contain at
	// most the blobs of a single pack which keeps payloads well below this.
	maxFramePayload = 64 * 1024 * 1024
)

type frameOp uint8

const (
	opUpsert frameOp = 1
	opDelete frameOp = 2
)

type wireFrame struct {
	Seq uint64      `json:"q"`
	Op  frameOp     `json:"o"`
	Rec *wireRecord `json:"r,omitempty"`
	Del string      `json:"d,omitempty"`
}

// encodeFrame serializes a frame. The returned slice is owned by the caller.
func encodeFrame(seq uint64, op frameOp, payload []byte) ([]byte, error) {
	if len(payload) > maxFramePayload {
		return nil, fmt.Errorf("ledger frame payload too large: %d", len(payload))
	}

	buf := make([]byte, frameHeader+len(payload)+frameTrailer)
	copy(buf[0:4], frameMagic)
	buf[4] = formatVersion
	buf[5] = byte(op)
	binary.LittleEndian.PutUint32(buf[6:10], uint32(len(payload)))
	copy(buf[frameHeader:], payload)

	sum := crc32.ChecksumIEEE(buf[:frameHeader+len(payload)])
	binary.LittleEndian.PutUint32(buf[frameHeader+len(payload):], sum)
	return buf, nil
}

type decodedFrame struct {
	seq uint64
	op  frameOp
	rec *Record
	del []byte // pack ID bytes when op == opDelete
}

// errTornFrame marks an unparseable frame at the end of a segment that may be
// the result of a crash while appending.
var errTornFrame = errors.New("torn frame")

// decodeFrame parses one frame from the start of data. It returns the decoded
// frame and the number of consumed bytes. A short buffer or any checksum or
// consistency error returns errTornFrame; callers decide whether the position
// is a tolerated segment tail or structural corruption.
func decodeFrame(data []byte) (decodedFrame, int, error) {
	if len(data) < frameHeader+frameTrailer {
		return decodedFrame{}, 0, errTornFrame
	}
	if string(data[0:4]) != frameMagic {
		return decodedFrame{}, 0, errTornFrame
	}
	if data[4] != formatVersion {
		return decodedFrame{}, 0, errTornFrame
	}
	op := frameOp(data[5])
	if op != opUpsert && op != opDelete {
		return decodedFrame{}, 0, errTornFrame
	}
	length := int(binary.LittleEndian.Uint32(data[6:10]))
	if length > maxFramePayload || length < 0 {
		return decodedFrame{}, 0, errTornFrame
	}
	total := frameHeader + length + frameTrailer
	if len(data) < total {
		return decodedFrame{}, 0, errTornFrame
	}

	want := crc32.ChecksumIEEE(data[:frameHeader+length])
	got := binary.LittleEndian.Uint32(data[frameHeader+length:])
	if want != got {
		return decodedFrame{}, 0, errTornFrame
	}

	var wf wireFrame
	if err := json.Unmarshal(data[frameHeader:frameHeader+length], &wf); err != nil {
		return decodedFrame{}, 0, errTornFrame
	}
	if wf.Seq == 0 {
		return decodedFrame{}, 0, errTornFrame
	}
	if frameOp(wf.Op) != op {
		return decodedFrame{}, 0, errTornFrame
	}

	df := decodedFrame{seq: wf.Seq, op: op}
	switch op {
	case opUpsert:
		if wf.Rec == nil || wf.Del != "" {
			return decodedFrame{}, 0, errTornFrame
		}
		rec, err := decodeRecord(*wf.Rec)
		if err != nil {
			return decodedFrame{}, 0, errTornFrame
		}
		if rec.Stage == StageNone || rec.Stage == StageRemoved {
			return decodedFrame{}, 0, errTornFrame
		}
		df.rec = &rec
	case opDelete:
		if wf.Rec != nil {
			return decodedFrame{}, 0, errTornFrame
		}
		id, err := restic.ParseID(wf.Del)
		if err != nil {
			return decodedFrame{}, 0, errTornFrame
		}
		df.del = id[:]
	}
	return df, total, nil
}
