# `engine_producer.py` — the engine simulator

Run this on your laptop. It pretends to be a vibration sensor bolted to a
single-shaft engine and publishes 500 readings a second over MQTT to the Pi.

```bash
pip install paho-mqtt numpy

python engine_producer.py --host 192.168.0.104 \
  --machine Karachi_ENG01 --seed 12345 \
  --schedule "healthy:120,imbalance:90,misalignment:90,bearing:120,looseness:90"
```

One file, no repo checkout, no dependency beyond `paho-mqtt` and `numpy`.

---

## 1. The machine it is pretending to be

Everything below follows from these numbers, so they are worth reading once.

A **single shaft at 1800 RPM**, which is **30 Hz** — call that `1x`. Bolted to
it is a rolling-element bearing with **8 balls** and a ball-to-pitch diameter
ratio of **0.35**. That geometry fixes where a bearing defect rings:

| Tone | Formula | Frequency | As a multiple of shaft |
|---|---|---|---|
| FTF (cage) | `0.5·(1−d/D)·f` | 9.75 Hz | 0.325x |
| BSF (ball spin) | `(D/2d)·(1−(d/D)²)·f` | 37.6 Hz | 1.254x |
| **BPFO (outer race)** | `(n/2)·(1−d/D)·f` | **78.0 Hz** | **2.600x** |
| BPFI (inner race) | `(n/2)·(1+d/D)·f` | 162.0 Hz | 5.400x |
| Vane pass | `7·f` | 210 Hz | 7.000x |
| Structural resonance | fixed | 180 Hz, Q≈8 | — |

**Every bearing tone is a non-integer multiple of shaft speed.** That is the
whole point. Shaft problems put energy at 1x, 2x, 3x — exact integers. Bearing
problems put it at 2.6x, 5.4x — between the harmonics. On a spectrogram you
tell them apart by where the line sits, which is exactly how it is done in the
field.

RPM is not held constant. It drifts ±3% over about 47 seconds, so every line
wobbles. A perfectly straight line on a spectrogram would mean a broken
simulator, not a healthy engine.

---

## 2. The five modes

Each fault is a different **shape**, not simply louder. A simulator where every
fault just raises the volume proves nothing.

Measured from your own running engines (13 clean minutes across three machines):

| mode | RMS | crest | kurtosis | axial ratio | skew |
|---|---|---|---|---|---|
| `healthy` | 0.0170 | 2.82 | 2.00 | 0.30 | 0.01 |
| `imbalance` | 0.0937 | 1.66 | **1.48** | 0.08 | 0.00 |
| `misalignment` | 0.0663 | 2.36 | 2.75 | **1.22** | −0.00 |
| `bearing` | 0.0634 | 3.22 | 2.79 | 0.08 | **−0.55** |
| `looseness` | 0.0678 | 3.33 | 2.96 | 0.19 | **+0.20** |

### `healthy`
The baseline. A modest 1x line, small harmonics, a low broadband floor. This is
what the condition score calibrates against, so a schedule should include a
decent stretch of it.

### `imbalance`
A heavy spot on the rotor. Physically the simplest fault: the mass pulls once
per revolution, so **the 1x line alone gains about 16 dB** and nothing else
moves much.

Two things worth knowing. The force grows with the *square* of speed, so the 1x
line brightens and dims visibly as the RPM drifts — no other mode does that.
And **kurtosis falls** (1.48 against a healthy 2.00), because a dominant pure
sine is *less* impulsive than broadband noise. If you expected a fault to raise
every statistic, this is the counterexample.

### `misalignment`
Two shafts coupled slightly off-axis. The coupling is loaded twice per
revolution, so **2x overtakes 1x** and a 3x appears. The giveaway is the
**axial** channel: vibration along the shaft roughly triples, so the axial ratio
goes from 0.30 to **1.22** — a 4x jump and the single cleanest discriminator in
the whole set.

### `bearing`
A defect on the bearing's outer race. Every time a ball rolls over it, the
structure is struck — 78 times a second — and the 180 Hz structural mode rings.
On the spectrogram you see a **bright band at 150–220 Hz with 78 Hz sidebands**,
about +37.6 dB above healthy. That is the strongest and most specific signature
any of these faults produces.

**Do not look for it in kurtosis.** Textbooks say bearing defects push kurtosis
above 6. Here it reaches 2.79 against a healthy 2.00, and that is correct, not a
bug: the resonance decays in `2Q/ω₀` = 14.1 ms while BPFO arrives every 12.8 ms,
so the rings overlap before they die. The result is continuous ringing, which
has the kurtosis of a tone rather than of an impulse. This is precisely why real
bearing diagnostics use envelope analysis instead. The detectable scalar here is
**skew**, which swings to −0.55.

### `looseness`
A slack mounting bolt. The structure rattles, so instead of a few clean tones you
get a **forest**: 1x through 7x all raised, plus a **0.5x subharmonic** at 15 Hz
below the main line, plus a lifted noise floor. Unmistakable at a glance, and the
0.5x line is the tell — nothing else produces one.

---

## 3. `--schedule`

```
--schedule "healthy:120,imbalance:90,misalignment:90,bearing:120,looseness:90"
             ^^^^^^^ ^^^
             mode    seconds in that mode
```

Comma-separated `mode:seconds` pairs. It **loops forever**, so the line above
cycles all five signatures and four transitions in about 8½ minutes and then
starts again. This is what makes an unattended demo worth watching.

Every switch **cross-fades over 4 seconds** (`--transition`). That is not
cosmetic: a step change in amplitude is a discontinuity that sprays broadband
energy across one STFT frame, drawing a bright vertical stripe that reads as a
fault. A gradient is also easier to see on a spectrogram than a hard edge.

Running several producers? Give each a different starting mode so they are not
all in the same fault at once:

```bash
--machine Karachi_ENG01 --schedule "healthy:120,imbalance:90,misalignment:90,bearing:120,looseness:90"
--machine Karachi_ENG02 --schedule "bearing:120,healthy:90,looseness:120,misalignment:90"
--machine Karachi_ENG03 --schedule "misalignment:120,healthy:90,imbalance:120,bearing:90,looseness:90"
```

---

## 4. Every flag

| Flag | Default | What it does |
|---|---|---|
| `--host` | `192.168.0.104` | The Pi's IP, where mosquitto runs. |
| `--port` | `1883` | MQTT port. |
| `--topic` | `engine/vibration` | Must match the Pi's `ENG_MQTT_TOPIC`. |
| `--machine` | `Karachi_ENG01` | **Must differ per producer.** See below. |
| `--rate` | `500` | Samples per second. Also the STFT's assumed rate. |
| `--rpm` | `1800` | Nominal shaft speed. 1x is this ÷ 60. |
| `--mode` | `healthy` | Starting mode, ignored if `--schedule` is given. |
| `--schedule` | — | `mode:seconds,...`, loops forever. |
| `--transition` | `4.0` | Cross-fade seconds between modes. |
| `--rpm-profile` | `drift` | `steady`, `drift`, or `steps`. |
| `--seconds` | — | Stop after this long. Default: run forever. |
| `--seed` | `12345` | RNG seed. **Differ it per producer.** |
| `--control-file` | `engine.mode` | Echo a mode into this to switch live. |
| `--dry-run` | off | Generate but publish nothing — checks your rate. |
| `--quiet` | off | Suppress the 10-second progress line. |

### `--machine` must differ per producer

Both pipeline models key on `[timestamp, machine]`. Two producers sharing a name
collide on that key, and since the fact table is an `INSERT … SELECT … GROUP BY`
the collision resolves via `min()` — **roughly half your readings vanish
silently**. Nothing errors. Use `Karachi_ENG01`, `..._ENG02`, and so on.

The part before the underscore becomes the `site` in the dimension table, so
`Karachi_ENG03` lands as site `Karachi`.

### `--seed` should differ per producer

Same seed means the identical waveform twice. It works, but two spectrograms
that are pixel-for-pixel copies rather defeat the point of running two engines.

### `--rpm-profile`

- **`steady`** — constant RPM. Lines are perfectly straight. Good for measuring,
  unrealistic to look at.
- **`drift`** *(default)* — ±3% over ~47 s plus a bounded random walk. Lines
  wobble; the 1x moves about ±1 bin and the 7x about ±6.5.
- **`steps`** — adds occasional load steps. Every harmonic kinks at the same
  instant, which is the single most convincing "this is a real machine"
  artefact.

---

## 5. Switching modes while it runs

**Control file** (works anywhere, survives a restart, works over ssh):

```bash
echo bearing > engine.mode
echo "healthy rpm=1500" > engine.mode     # a mode and a speed change
```

Polled once per 100 ms block with a single `os.stat`, and only re-read when the
mtime changes, so it costs nothing.

**Signal** (Linux/macOS only — Windows has no `SIGUSR1`):

```bash
kill -USR1 <pid>      # advance to the next mode
```

---

## 6. Reading the output

```
  bearing        500.0 msg/s  rpm 1857.4  seq 5,000
```

Printed every 10 seconds: current mode, achieved rate, current shaft speed, and
the running sample index. A `BEHIND` marker appears if the achieved rate falls
below 97% of the requested one.

**`seq`** is a monotonic per-run sample index, and it is what makes "are we
really getting 500 Hz" an exact measurement rather than a guess. The Pi computes

```
loss = 1 − count(*) / (max(seq) − min(seq) + 1)
```

Check it any time without touching the catalog:

```bash
curl -s 192.168.0.104:8090/api/status | python3 -m json.tool | grep -A8 machines_landing
```

Measured on this setup with three producers: **500.0 Hz each, 0.000% loss, 0
duplicates.**

---

## 7. Before you blame the network

Check the laptop can actually generate at rate, with no broker involved:

```bash
python engine_producer.py --dry-run --seconds 20
```

You want `500.0 msg/s` and no `BEHIND`. If that is fine but loss appears once
publishing, the problem is the link or the broker — escalate in this order:
raise mosquitto's in-flight limits, batch readings per message, then drop to
`--rate 250` (keeps 1x–4x and BPFO, loses BPFI).

---

## 8. Where to look on the dashboard

| Question | Where |
|---|---|
| Is anything wrong at all? | **Engine condition** score, or the RMS chart |
| Which fault is it? | The condition chart's **driver** label |
| Is it misalignment? | `axial ratio` driver — the table column jumps to ~1.2 |
| Is it a bearing? | The **spectrogram** — a 150–220 Hz band. Not kurtosis. |
| Is it imbalance? | Kurtosis chart — it goes **down** |
| Is it looseness? | Spectrogram — a harmonic forest plus 0.5x at 15 Hz |
| Are we losing data? | `machines_landing` per-machine loss |

Two caveats on the condition score. It calibrates against each machine's own
quietest minutes, so it needs a few minutes of running before it means anything
— `baseline_minutes` in the legend tells you how many it has. And a machine that
is *never* healthy while observed will calibrate to its own fault and read
normal.

The `mode` field is the simulator telling you ground truth. **A real machine has
no such column** — it is the thing you are trying to infer, not an input. It
exists here only so the whole chain can be checked against what was commanded.
