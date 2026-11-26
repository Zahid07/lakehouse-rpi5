#!/usr/bin/python3
"""ClickHouse executable UDF: stft_db(values Array(Float64)) -> Array(Float32).

One minute of vib_r samples, sorted by time, in; the flattened magnitude
spectrogram in dB re 1 g out. The maths is lakehouse-rpi5/engine_pipeline/
spectro.py unchanged:

    fs 500 Hz, frame 512 (Hann), hop 500 -> 59 frames x 257 bins per minute
    per-frame mean removed; |rfft| * 2/sum(w); bins 0 and Nyquist not doubled
    20*log10, floored at -120 dB, rounded to 0.1 dB
    layout: spec_db[f*257 + b + 1] (1-based in SQL); bin b is b*fs/frame Hz

Fewer samples than one frame gives an empty array: a partial minute that
short is no answer, not a zero answer.

Protocol: ClickHouse runs this as a persistent process (executable_pool) and
talks RowBinary, which carries each array as a length followed by raw
little-endian floats -- read straight into numpy with no text parsing. With
send_chunk_header, each chunk starts with its row count on a line of its own,
and exactly that many result rows must be written back.
"""

import sys

import numpy as np

FS = 500.0
FRAME = 512
HOP = 500
N_BINS = FRAME // 2 + 1
WINDOW = np.hanning(FRAME)
GAIN = WINDOW.sum()
DB_FLOOR = -120.0


def stft_db(v: np.ndarray) -> np.ndarray:
    if v.size < FRAME:
        return np.empty(0, dtype=np.float64)
    n_frames = (v.size - FRAME) // HOP + 1
    frames = np.lib.stride_tricks.sliding_window_view(v, FRAME)[::HOP][:n_frames]
    frames = frames - frames.mean(axis=1, keepdims=True)
    mag = np.abs(np.fft.rfft(frames * WINDOW, axis=1)) * (2.0 / GAIN)
    mag[:, 0] *= 0.5
    if FRAME % 2 == 0:
        mag[:, -1] *= 0.5
    db = 20.0 * np.log10(np.maximum(mag, 1e-12))
    np.maximum(db, DB_FLOOR, out=db)
    return np.round(db, 1).ravel()


def read_varint(stream) -> int:
    shift = result = 0
    while True:
        byte = stream.read(1)
        if not byte:
            raise EOFError
        b = byte[0]
        result |= (b & 0x7F) << shift
        if b < 0x80:
            return result
        shift += 7


def write_varint(out: bytearray, n: int) -> None:
    while n >= 0x80:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)


def main() -> None:
    stdin, stdout = sys.stdin.buffer, sys.stdout.buffer
    while True:
        header = stdin.readline()
        if not header:
            return                                  # ClickHouse closed the pipe
        rows = int(header)
        out = bytearray()
        for _ in range(rows):
            n = read_varint(stdin)
            values = np.frombuffer(stdin.read(n * 8), dtype="<f8")
            spec = stft_db(values).astype("<f4")
            write_varint(out, spec.size)
            out += spec.tobytes()
        stdout.write(out)
        stdout.flush()


if __name__ == "__main__":
    main()
