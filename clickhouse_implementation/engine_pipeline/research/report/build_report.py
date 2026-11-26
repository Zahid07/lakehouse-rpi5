#!/usr/bin/env python3
"""Build the DuckDB vs ClickHouse research report as a PDF.

    python3 research/report/build_report.py

Writes report.html, renders it with headless Chromium, reads back the page on
which each heading landed (pdftotext), fills in the contents page numbers and
renders again. Output: research/DuckDB_vs_ClickHouse_RPi5_report.pdf.
Figures are drawn here as SVG from the numbers in research/runs/comparison/.
"""
import html
import re
import subprocess
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
HTML_OUT = HERE / "report.html"
PDF_OUT = HERE.parent / "DuckDB_vs_ClickHouse_RPi5_report.pdf"

TITLE = "Real-Time Sensor Analytics on a Raspberry Pi 5"
SUBTITLE = "A Comparison of DuckDB and ClickHouse on an Uncooled Edge Device"
AUTHOR = "Muhammad Zahid"
SUPERVISORS = "Andreas Lundell and Fredrik Hellman"
UNIVERSITY = "Åbo Akademi University"
DATE = "November 2026"

DUCK = "#2f6db5"      # DuckDB colour in every figure
CH = "#d9822b"        # ClickHouse colour in every figure
GRID = "#dddddd"
INK = "#222222"
QUIET = "#666666"

# ---------------------------------------------------------------- headings
toc = []          # (level, number, title, anchor)


def h2(num, title):
    anchor = "s" + num.replace(".", "-")
    toc.append((2, num, title, anchor))
    return f'<h2 id="{anchor}">{num}&nbsp;&nbsp;{html.escape(title)}</h2>'


def h3(num, title):
    anchor = "s" + num.replace(".", "-")
    toc.append((3, num, title, anchor))
    return f'<h3 id="{anchor}">{num}&nbsp;&nbsp;{html.escape(title)}</h3>'


def table(caption, head, rows, note=None, cls=""):
    th = "".join(f"<th>{h}</th>" for h in head)
    tr = "".join("<tr>" + "".join(f"<td>{c}</td>" for c in r) + "</tr>" for r in rows)
    n = f'<div class="note">{note}</div>' if note else ""
    return f'<figure class="tbl {cls}"><figcaption>{caption}</figcaption><table><thead><tr>{th}</tr></thead><tbody>{tr}</tbody></table>{n}</figure>'


def figure(svg, caption):
    return f'<figure class="fig">{svg}<figcaption>{caption}</figcaption></figure>'


def text(x, y, s, size=12, anchor="start", fill=INK, weight="normal"):
    return (f'<text x="{x}" y="{y}" font-size="{size}" text-anchor="{anchor}" fill="{fill}" '
            f'font-weight="{weight}">{html.escape(s)}</text>')


# ---------------------------------------------------------------- figures
def fig_architecture():
    def box(x, y, w, h, fill="none"):
        return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="7" fill="{fill}" stroke="#888" stroke-width="1.2"/>'

    def block(x, y, name, l1, l2):
        return (box(x, y, 300, 70, "#ffffff") + text(x + 12, y + 23, name, 13, weight="bold")
                + text(x + 12, y + 41, l1, 11.5, fill=QUIET) + text(x + 12, y + 57, l2, 11.5, fill=QUIET))

    arrow = '<path d="{d}" fill="none" stroke="#888" stroke-width="1.3" marker-end="url(#ar)"/>'
    p = ['<svg viewBox="0 0 720 520" xmlns="http://www.w3.org/2000/svg" font-family="Liberation Sans, sans-serif">',
         '<defs><marker id="ar" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto"><path d="M0 0L10 5L0 10z" fill="#888"/></marker></defs>',
         box(190, 10, 340, 54), text(360, 33, "6 sensor producers on the laptop", 13, "middle", weight="bold"),
         text(360, 51, "500 readings/s each, 50 readings per MQTT message, QoS 1", 11.5, "middle", QUIET),
         box(190, 90, 340, 54), text(360, 113, "RabbitMQ with the MQTT plugin (on the Pi)", 13, "middle", weight="bold"),
         text(360, 131, "The same broker and message stream for both pipelines", 11.5, "middle", QUIET),
         arrow.format(d="M360 64V90"), arrow.format(d="M360 144V160H186V180"), arrow.format(d="M360 160H534V180"),
         box(20, 180, 332, 330, "#f3f6fb"), text(36, 203, "DuckDB pipeline", 13.5, weight="bold", fill=DUCK),
         block(36, 216, "Ingest process", "Subscribes over MQTT and writes Parquet", "landing files (5,000 rows or 10 s)"),
         block(36, 316, "duckstream cycle, every 2 s", "Finds new landing files, runs each", "model, marks the files as consumed"),
         block(36, 416, "DuckLake tables (Parquet)", "Fact, minute health, spectrogram;", "only one process may open the catalog"),
         box(368, 180, 332, 330, "#fbf5ee"), text(384, 203, "ClickHouse pipeline", 13.5, weight="bold", fill=CH),
         block(384, 216, "RabbitMQ table engine", "Reads an AMQP queue all the time and", "inserts each batch as soon as it arrives"),
         block(384, 316, "Raw table + materialized views", "A per-minute summary is updated on", "every insert; raw data kept for 7 days"),
         block(384, 416, "Scheduled jobs + live views", "Store finished minutes (health, STFT);", "the newest minutes are calculated live"),
         arrow.format(d="M186 286V316"), arrow.format(d="M186 386V416"),
         arrow.format(d="M534 286V316"), arrow.format(d="M534 386V416"),
         "</svg>"]
    return "".join(p)


def fig_latency():
    duck = [(0, 10039), (30, 4180), (60, 1199), (90, 4261), (120, 4040), (150, 3943), (180, 6641),
            (210, 5071), (240, 6357), (270, 7637), (300, 8506), (330, 10576), (360, 16596)]
    x = lambda t: 70 + t / 360 * 600
    y = lambda ms: 270 - ms / 18000 * 220
    p = ['<svg viewBox="0 0 700 320" xmlns="http://www.w3.org/2000/svg" font-family="Liberation Sans, sans-serif">',
         f'<rect x="{x(0)}" y="40" width="{x(150) - x(0)}" height="230" fill="#f2f2f2"/>',
         text(x(4), 56, "Sensors added, one every 30 min", 11, fill=QUIET),
         text(x(156), 56, "All 6 sensors, constant load", 11, fill=QUIET)]
    for v in (0, 5000, 10000, 15000):
        p.append(f'<line x1="70" x2="670" y1="{y(v)}" y2="{y(v)}" stroke="{GRID}"/>')
        p.append(text(62, y(v) + 4, f"{v // 1000} s", 11, "end", QUIET))
    for h in range(7):
        p.append(text(x(h * 60), 290, f"{h} h", 11, "middle", QUIET))
    p.append(text(370, 312, "Time into the run", 11, "middle", QUIET))
    p.append(f'<polyline points="{" ".join(f"{x(t)},{y(3)}" for t, _ in duck)}" fill="none" stroke="{CH}" stroke-width="2.5"/>')
    p.append(f'<polyline points="{" ".join(f"{x(t)},{y(ms)}" for t, ms in duck)}" fill="none" stroke="{DUCK}" stroke-width="2"/>')
    p += [f'<circle cx="{x(t)}" cy="{y(ms)}" r="3.2" fill="{DUCK}"/>' for t, ms in duck]
    p.append(text(x(360) - 8, y(16596) + 4, "DuckDB 16.6 s", 12, "end", INK, "bold"))
    p.append(text(x(360), y(3) - 8, "ClickHouse 3 ms throughout", 12, "end", INK, "bold"))
    p.append(text(x(0) + 10, y(10039) - 8, "10.0 s with one sensor (10 s file flush)", 11, fill=QUIET))
    p.append("</svg>")
    return "".join(p)


def fig_cpu():
    ch = [34.2, 105.3, 115.9, 128.7, 114.4, 112.5, 118.9]
    dk = [40.6, 61.4, 106.8, 151.4, 158.6, 162.1, 165.3]
    band, bw = 600 / 7, 28
    gx = lambda i: 70 + i * band
    y = lambda v: 260 - v / 200 * 210
    p = ['<svg viewBox="0 0 700 320" xmlns="http://www.w3.org/2000/svg" font-family="Liberation Sans, sans-serif">',
         f'<rect x="470" y="8" width="12" height="12" fill="{CH}"/>', text(488, 18, "ClickHouse", 12),
         f'<rect x="580" y="8" width="12" height="12" fill="{DUCK}"/>', text(598, 18, "DuckDB", 12),
         f'<line x1="70" x2="670" y1="{y(100)}" y2="{y(100)}" stroke="#999" stroke-dasharray="4 4"/>',
         text(64, y(100) + 4, "1 core", 11, "end", QUIET),
         f'<line x1="70" x2="670" y1="260" y2="260" stroke="#999"/>']
    for i in range(7):
        for v, dx, c in ((ch[i], band / 2 - bw - 2, CH), (dk[i], band / 2 + 2, DUCK)):
            p.append(f'<rect x="{gx(i) + dx}" y="{y(v)}" width="{bw}" height="{260 - y(v)}" fill="{c}"/>')
            p.append(text(gx(i) + dx + bw / 2, y(v) - 5, f"{round(v)}", 10.5, "middle"))
        p.append(text(gx(i) + band / 2, 278, "Idle" if i == 0 else str(i), 11.5, "middle", QUIET))
    p.append(text(370, 302, "Sensors connected", 11, "middle", QUIET))
    p.append("</svg>")
    return "".join(p)


def fig_cycle():
    rows = [("Hour 2", "3–4 sensors", 3774, 386), ("Hour 3", "5–6 sensors", 5785, 540),
            ("Hour 4", "6 sensors", 7760, 664), ("Hour 5", "6 sensors", 9748, 782), ("Hour 6", "6 sensors", 11559, 891)]
    band, bw = 600 / 5, 56
    gx = lambda i: 70 + i * band + band / 2 - bw / 2
    y = lambda ms: 250 - ms / 13000 * 210
    p = ['<svg viewBox="0 0 700 310" xmlns="http://www.w3.org/2000/svg" font-family="Liberation Sans, sans-serif">',
         f'<rect x="420" y="8" width="12" height="12" fill="{DUCK}"/>', text(438, 18, "Running the models", 12),
         '<rect x="570" y="8" width="12" height="12" fill="#c9c9c9"/>', text(588, 18, "Everything else", 12)]
    for v in (0, 4000, 8000, 12000):
        p.append(f'<line x1="70" x2="670" y1="{y(v)}" y2="{y(v)}" stroke="{GRID}"/>')
        p.append(text(62, y(v) + 4, f"{v // 1000} s", 11, "end", QUIET))
    for i, (h, s, cyc, work) in enumerate(rows):
        p.append(f'<rect x="{gx(i)}" y="{y(cyc)}" width="{bw}" height="{y(work) - y(cyc)}" fill="#c9c9c9"/>')
        p.append(f'<rect x="{gx(i)}" y="{y(work)}" width="{bw}" height="{250 - y(work)}" fill="{DUCK}"/>')
        p.append(text(gx(i) + bw / 2, y(cyc) - 6, f"{cyc / 1000:.1f} s", 11.5, "middle", INK, "bold"))
        p.append(text(gx(i) + bw + 4, y(work) + 4, f"{work / 1000:.2f} s", 10, "start", DUCK))
        p.append(text(gx(i) + bw / 2, 268, h, 11.5, "middle", QUIET))
        p.append(text(gx(i) + bw / 2, 283, s, 10.5, "middle", QUIET))
    p.append("</svg>")
    return "".join(p)


# ---------------------------------------------------------------- body
def body():
    b = []
    a = b.append

    a(h2("1", "Abstract"))
    a("""<p>Small computers such as the Raspberry Pi 5 are cheap enough to sit next to every machine in a
    factory. If they can store and analyse sensor data on their own, a plant does not need to send every
    reading to the cloud. In this study I compare two ways of doing this on a Raspberry Pi 5 that has no
    cooling at all.</p>
    <p>The first pipeline uses DuckDB, together with DuckLake and duckstream, a small streaming engine I
    built for it. The second uses RabbitMQ and ClickHouse. Both pipelines receive readings from six
    simulated engine sensors that each send 500 readings per second, and both compute the same health
    statistics and spectrograms. I ran each pipeline for six hours, adding one sensor every 30 minutes, and
    measured data loss, delay, CPU, memory, temperature and disk use.</p>
    <p>Both pipelines stored all 51,300,100 readings without losing a single one. The difference was in how
    they behaved over time. ClickHouse made each reading available in about 3 milliseconds, and this stayed
    the same for the whole run. DuckDB took about 7 seconds, and its delay kept growing until it reached
    16.6 seconds at the end. My analysis shows that this growth came from the cost of finding new files,
    which rises as files pile up, and not from the calculations themselves. DuckDB also used 39% more CPU
    at full load, which kept the bare Pi at its temperature limit for about half of that time. DuckDB did
    have two real advantages: it needed far less memory when idle, and it stored its per-minute results
    sooner. For continuous real-time work, ClickHouse was the better choice.</p>""")

    a(h2("2", "Introduction"))
    a("""<p>This project did not start as a comparison. It started as a single pipeline, and the comparison
    grew out of it. This section explains how that happened and what questions the study tries to answer.</p>""")
    a(h3("2.1", "How the work started"))
    a("""<p>I first built an engine monitoring pipeline on DuckDB. The idea was simple. Sensors on each engine
    send vibration readings over MQTT, the Pi stores them, and a dashboard shows the health of every engine
    in near real time.</p>
    <p>DuckDB is an analytical database. It is very good at answering questions about data that is already
    stored, but it has no built-in way to keep processing data as it arrives. To fill that gap I wrote
    duckstream, a small micro-batch engine. Every two seconds it looks for new data files, runs each
    calculation on them, and records which files it has already processed. With duckstream in place, the
    pipeline produced a star schema, per-minute health statistics, a spectrogram for each minute and an
    anomaly score, all shown on a live dashboard.</p>""")
    a(h3("2.2", "Why I tried ClickHouse"))
    a("""<p>Once the DuckDB version worked, I started to wonder whether a database built for real-time data could
    do the same job with less custom code. ClickHouse looked like a good candidate, because it can read from
    a message queue directly and it can update summary tables as new rows arrive. So I ported the whole
    pipeline to RabbitMQ and ClickHouse and kept every calculation the same. I then checked the results
    against the original numpy code to make sure both versions gave identical answers.</p>
    <p>Having two working versions of the same pipeline made the next step obvious: run them under the same
    load and see which one copes better. That comparison is the subject of this report.</p>""")
    a(h3("2.3", "Research questions"))
    a("""<p>The comparison is built around five questions:</p>
    <ol class="rq">
    <li><b>Completeness.</b> Does each pipeline store every reading when 3,000 readings arrive each second?</li>
    <li><b>Delay.</b> How long does a reading take to become available, and does this change during a long run?</li>
    <li><b>Resources.</b> How much CPU, memory and disk does each pipeline use?</li>
    <li><b>Scaling.</b> How do these numbers change as sensors are added, from one to six?</li>
    <li><b>Heat.</b> How hot does an uncooled Pi 5 get under each pipeline, and does it slow itself down?</li>
    </ol>""")
    a(h3("2.4", "How this report is organised"))
    a("""<p>Section 3 introduces the tools. Section 4 describes how the two pipelines are built, and Section 5
    covers the problems I ran into while building them, since some of them affect how fair the comparison
    is. Section 6 explains how the experiment was set up. Section 7 presents the results, and Section 8
    analyses them to explain why the two systems behaved so differently. Section 9 discusses which system
    is better and in which situations. Sections 10 and 11 cover the limits of the study and the
    conclusions.</p>""")

    a(h2("3", "Background"))
    a("""<p>The two pipelines are built on tools that work in opposite ways. DuckDB is a library that a program
    calls when it needs it, while ClickHouse is a server that runs all the time and reacts to data as it
    arrives. Table 1 sums up the differences that matter for this study, and the rest of this section
    describes each tool in turn.</p>""")
    a(table("Table 1. The two stacks compared",
            ["", "DuckDB + DuckLake + duckstream", "RabbitMQ + ClickHouse"],
            [["Type", "Library inside a Python process", "Database server"],
             ["Storage", "Parquet files and a DuckLake catalog", "MergeTree parts, merged in the background"],
             ["Writers", "One process at a time", "Many readers and writers at once"],
             ["Streaming", "Added by duckstream (micro-batches)", "Built in (queue engine + materialized views)"],
             ["Scheduled work", "duckstream cycle every 2 s", "Refreshable materialized views"],
             ["FFT / STFT", "numpy, inside the pipeline", "numpy, as a user-defined function"],
             ["Memory when idle", "About 150 MB", "About 1 GB"]]))
    a(h3("3.1", "DuckDB and DuckLake"))
    a("""<p>DuckDB is an analytical database that runs inside another program, in the same way SQLite does, so
    there is no server to install or manage [1]. DuckLake adds a catalog on top of plain Parquet files,
    which gives the data snapshots and safe transactions [2]. One limit of this setup shaped much of my
    design: only one process can open a DuckLake catalog at a time. The ingest, the pipeline and the
    dashboard therefore have to take turns.</p>""")
    a(h3("3.2", "duckstream"))
    a("""<p>duckstream is the streaming layer I wrote for the DuckDB pipeline, and it has two parts. The ingest
    process subscribes to MQTT and writes readings to Parquet “landing” files, closing a file every 5,000
    rows or every 10 seconds, whichever comes first. The pipeline process then runs in a loop every 2
    seconds. In each cycle it finds the landing files it has not seen yet, runs each model on them, commits
    the results and marks the files as consumed. This design is easy to follow and easy to debug. As
    Section 8 will show, however, the cost of finding new files grows as more files pile up.</p>""")
    a(h3("3.3", "ClickHouse"))
    a("""<p>ClickHouse is a column-oriented database server built for fast analytics on large amounts of data
    [3]. Three of its features matter here. Its RabbitMQ table engine reads messages from a queue and turns
    them into rows. Materialized views run a query on every insert, so summary tables stay up to date
    without any extra job. Refreshable materialized views run a query on a schedule, and I used them to
    store minutes that have finished.</p>""")
    a(h3("3.4", "RabbitMQ and MQTT"))
    a("""<p>MQTT is a lightweight messaging protocol that is widely used by sensors. RabbitMQ is a message broker,
    and its MQTT plugin lets sensors publish to it directly [4]. Both pipelines used the same RabbitMQ
    broker on the Pi. The sensors published with QoS 1, which means the broker confirms every message, so a
    sensor knows when its data has arrived safely.</p>""")

    a(h2("4", "System design"))
    a("""<p>Both pipelines start in the same place and end with the same tables. They differ only in the middle,
    in how a reading travels from the broker into storage. Figure 1 shows both side by side.</p>""")
    a(figure(fig_architecture(), "Figure 1. The two pipelines share the sensors and the broker but differ in how readings reach the tables."))
    a(h3("4.1", "The DuckDB pipeline"))
    a("""<p>In the DuckDB pipeline, a reading first lands in a Parquet file written by the ingest process. Up to
    10 seconds later the file is closed, and the next duckstream cycle picks it up, runs the models and
    writes the results to DuckLake tables. This means every reading is written to disk twice: once in a
    landing file and once in the lake.</p>""")
    a(h3("4.2", "The ClickHouse pipeline"))
    a("""<p>In the ClickHouse pipeline, the server reads the RabbitMQ queue all the time and inserts each batch
    of messages as soon as it arrives. A materialized view then updates a small per-minute summary on every
    insert. Scheduled jobs use this summary to decide which minutes have finished and need their health
    statistics and spectrogram stored. Because the jobs only read the small summary, their cost does not
    grow as the raw table grows.</p>""")
    a(h3("4.3", "Data model"))
    a("""<p>Both pipelines build the same star schema, so the results can be compared one to one:</p>
    <ul>
    <li><b>Fact table</b>: one row per reading, with the machine, timestamp, sequence number, the vibration
    channels, RPM and the audit columns used in this study.</li>
    <li><b>Machine dimension</b>: details about each machine, with their history kept as a type 2 slowly
    changing dimension.</li>
    <li><b>Minute health</b>: RMS, peak, crest factor, kurtosis, skew, axial ratio and RPM statistics for each
    machine and minute.</li>
    <li><b>Minute spectrogram</b>: a short-time Fourier transform for each machine and minute (500 Hz,
    512-sample Hann window, hop of 500), computed with numpy in both pipelines.</li>
    <li><b>Anomaly score</b>: each minute compared with the machine’s own median and median absolute
    deviation.</li>
    </ul>""")
    a(h3("4.4", "Stored and live results"))
    a("""<p>For the ClickHouse version I followed one rule: only store a result once it will no longer change.
    A minute of health data is stored once it is more than 10 minutes old, and a spectrogram is stored once
    its minute has closed. For anything newer, the views calculate the result live from the raw readings.
    This keeps the stored tables small and avoids recalculating minutes that are already final. duckstream
    works differently. It stores every model’s output in each cycle, as soon as the new files have been
    processed.</p>""")

    a(h2("5", "Problems found while building"))
    a("""<p>Building the two pipelines was not smooth, and a few of the problems had a direct effect on how fair
    the final comparison would be. Table 2 lists the main ones. Most of them were in the message path or in
    the dashboard, not in the databases themselves.</p>""")
    a(table("Table 2. Main problems, their causes and fixes",
            ["Problem", "Cause", "Fix", "Applied to"],
            [["20–80% of readings lost with 6 sensors", "Heavy dashboard queries starved ClickHouse, so RabbitMQ slowed the sensors down and their buffers overflowed",
              "STFT moved to a numpy function (800 ms down to 4 ms per minute), simpler anomaly query, slower dashboard refresh", "ClickHouse"],
             ["12–29% loss and about 48 s of delay", "One reading per message; confirms took about 1.4 s, so each sensor was capped near 400 readings/s",
              "50 readings per message and up to 10,000 messages in flight", "Both"],
             ["Some stored minutes skipped", "One shared progress marker for all machines", "Progress tracked per machine", "ClickHouse"],
             ["Two hours of data discarded", "The DuckDB ingest was stuck at a password prompt nobody could see", "Ingest stops at once without a password; start script checks the subscription", "DuckDB"],
             ["Runs interrupted", "Power drop on the Pi, laptop Python environment not active, WiFi drop", "Runs restarted; launcher checks its packages first", "Both"],
             ["False delay of 0.5–2 s", "Laptop clock not in sync with the Pi", "Delay measured on the Pi clock only", "Both"]]))
    a("""<p>The most important fix was batching. At first, each MQTT message carried a single reading. Under
    load, RabbitMQ took about 1.4 seconds to confirm each message, and each sensor could only have 500
    unconfirmed messages waiting at once. This capped every sensor at about 400 readings per second, which
    is below the 500 it needed, so readings piled up and were eventually dropped. Sending 50 readings in
    each message solved the problem. I made the same change in both pipelines, so they received exactly the
    same stream of messages.</p>
    <p>The DuckDB run also needed four attempts before I had a clean six-hour run. None of the failures were
    caused by DuckDB itself, but each one taught me something, and I added checks to the start scripts so
    that a run cannot begin unless every part is really connected.</p>""")

    a(h2("6", "Experimental setup"))
    a("""<p>To make the comparison fair, each pipeline ran once for six hours on the same Pi, with the same
    sensors and the same schedule, while the other pipeline was switched off.</p>""")
    a(h3("6.1", "Hardware and software"))
    a("""<p>Table 3 lists the setup. The Pi had no heatsink, no fan and no case. I left it bare on purpose,
    because many edge devices end up installed this way, and because it makes heat a real part of the
    comparison.</p>""")
    a(table("Table 3. Hardware and software", ["Item", "Details"],
            [["Board", "Raspberry Pi 5, 4 GB RAM, 4 cores at 2.4 GHz"],
             ["Cooling", "None (no heatsink, no fan, no case)"],
             ["Storage", "SD card"],
             ["Operating system", "Debian 13 (trixie), 64-bit"],
             ["Broker", "RabbitMQ with the MQTT plugin, on the Pi"],
             ["Network", "Laptop and Pi on the same 5 GHz WiFi"],
             ["Sensors", "Windows 11 laptop, one Python process per engine"],
             ["DuckDB stack", "DuckDB 1.5.5, DuckLake, duckstream"],
             ["ClickHouse stack", "ClickHouse 26.9, numpy STFT function"]], cls="narrow"))
    a(h3("6.2", "Workload"))
    a("""<p>Six simulated engines, named Karachi_ENG01 to Karachi_ENG06, produced data from the same signal
    model in both runs. Each sent 500 readings per second, which gives 3,000 readings per second at full
    load. A launcher on the laptop started one sensor every 30 minutes, so all six were running together for
    the last three and a half hours, and it stopped all of them at the six-hour mark. Each run began with
    about 10 minutes of idle time to record a baseline. Table 4 gives the times of the two runs.</p>""")
    a(table("Table 4. The two runs", ["Run", "Start (UTC)", "End (UTC)", "Settings"],
            [["ClickHouse (ch_run1)", "7 Oct 2026, 19:54", "8 Oct 2026, 01:54", "Default design"],
             ["DuckDB (duck_run1)", "12 Oct 2026, 12:34", "12 Oct 2026, 18:34", "As designed: 10 s file flush, 2 s cycle"]]))
    a(h3("6.3", "What I measured"))
    a("""<p>I added the same audit columns to both pipelines. Each reading records when it was sent on the
    laptop, when it was received on the Pi and when it was stored and ready to query. These are rolled up
    per machine and minute into an identical export for each run. To check for loss, every sensor numbers
    its readings, so a gap in the numbers shows a missing reading.</p>
    <p>A separate monitoring script sampled the Pi every 10 seconds during both runs. It recorded total and
    per-process CPU and memory, temperature, clock speed, the firmware’s throttling flags, and disk and
    network traffic. In the results, each sample is grouped by how many sensors were connected at the
    time.</p>""")
    a(h3("6.4", "A note on clocks"))
    a("""<p>The laptop and the Pi clocks were not synchronised. In the ClickHouse run the laptop clock was
    about 0.6 seconds ahead of the Pi, and in the DuckDB run it was about 1.1 seconds behind. Any delay
    measured from the laptop to the Pi is therefore off by up to a second. To avoid this, the main delay
    measure in this report is the time from <i>received</i> to <i>stored</i>, which uses only the Pi’s
    clock.</p>""")

    a(h2("7", "Results"))
    a("""<p>Both pipelines did the job: neither lost any data. Where they differed was in how quickly the data
    became available, how much of the Pi they used, and how this changed as the run went on. This section
    presents the results question by question.</p>""")
    a(h3("7.1", "Completeness"))
    a(table("Table 5. Readings stored", ["Pipeline", "Readings", "Machines", "Missing", "Duplicates"],
            [["ClickHouse", "51,300,100", "6", "0", "0"], ["DuckDB", "51,300,100", "6", "0", "0"]], cls="narrow"))
    a("""<p>Every reading that was sent was stored, by both pipelines and at every sensor count. The counts per
    engine also match exactly, from 10,800,050 readings for ENG01, which ran for six hours, down to
    6,300,000 for ENG06, which ran for three and a half.</p>""")
    a(h3("7.2", "Delay"))
    a("""<p>Delay is where the two pipelines differed most. Figure 2 shows the median delay in each 30-minute
    block of the run.</p>""")
    a(figure(fig_latency(), "Figure 2. Median time from received to stored, per 30-minute block. ClickHouse stays at 3 ms; DuckDB grows during the constant-load period."))
    a(table("Table 6. Received-to-stored time by number of sensors",
            ["Sensors", "ClickHouse median", "ClickHouse 95th pct.", "DuckDB median", "DuckDB 95th pct.", "DuckDB max"],
            [["1", "3 ms", "4 ms", "10.0 s", "10.1 s", "10.2 s"], ["2", "3 ms", "7 ms", "4.2 s", "4.7 s", "4.8 s"],
             ["3", "3 ms", "7 ms", "1.2 s", "3.5 s", "4.0 s"], ["4", "3 ms", "8 ms", "4.3 s", "7.0 s", "7.1 s"],
             ["5", "3 ms", "6 ms", "4.0 s", "6.2 s", "6.3 s"], ["6", "3 ms", "7 ms", "6.9 s", "13.1 s", "16.6 s"]],
            note="Last reading of each machine-minute, measured on the Pi clock. ClickHouse never exceeded 11 ms."))
    a("""<p>ClickHouse stayed at 3 ms from the first sensor to the last hour. DuckDB’s delay followed a different
    pattern. With one sensor it was about 10 seconds, because a landing file closes after 5,000 rows or 10
    seconds and one sensor needs 10 seconds to send 5,000 rows. As more sensors joined, files filled faster
    and the delay dropped to 1.2 seconds with three sensors. From four sensors on, the 2-second cycle could
    no longer keep up, and the delay started to climb. It also kept climbing during the last
    three and a half hours, even though the load did not change. Section 8.1 looks at why.</p>
    <p>The picture is different for the derived results. Table 7 shows how long each pipeline took to make
    the per-minute health statistics and spectrograms available.</p>""")
    a(table("Table 7. Derived results with 6 sensors (median)", ["Result", "ClickHouse", "DuckDB"],
            [["Minute health available to queries", "About 0 s (calculated live)", "8.0 s"],
             ["Minute health stored", "600 s (by design)", "8.0 s"],
             ["Spectrogram stored", "20 s (job interval)", "7.8 s"]], cls="narrow"))
    a("""<p>DuckDB stores derived minutes sooner, because each cycle runs every model in one pass. ClickHouse
    makes health available straight away by calculating it live, but only stores it once the minute is
    final. Both approaches give the dashboard current values; they differ in when the values are written to
    disk.</p>""")
    a(h3("7.3", "CPU"))
    a(figure(fig_cpu(), "Figure 3. Average CPU of each pipeline’s own processes, as a percentage of one core (the Pi has four)."))
    a("""<p>Figure 3 shows that ClickHouse’s CPU use was almost flat once data was flowing. It jumped from 34% to
    105% of a core when the first sensor connected, and then rose only to 119% with all six. DuckDB started
    lower, at 61% with one sensor, but climbed with every sensor that joined and reached 165% with six. From
    three sensors on, DuckDB used between 18% and 44% more CPU than ClickHouse. Across the whole Pi, total
    CPU with six sensors was 31.2% for ClickHouse and 43.7% for DuckDB.</p>""")
    a(h3("7.4", "Memory"))
    a(table("Table 8. Memory use", ["Sensors", "ClickHouse process", "DuckDB processes", "Pi memory used, ClickHouse", "Pi memory used, DuckDB"],
            [["Idle", "979 MB", "149 MB", "43.2%", "33.9%"], ["1", "1,045 MB", "261 MB", "44.3%", "37.1%"],
             ["3", "1,032 MB", "411 MB", "45.0%", "42.2%"], ["6", "1,013 MB", "906 MB", "44.9%", "52.0%"],
             ["Peak in run", "1,201 MB", "1,843 MB", "49.3%", "69.1%"]]))
    a("""<p>ClickHouse used about 1 GB whatever the load. DuckDB started almost seven times smaller, but its
    memory grew with both load and time and peaked at 1.8 GB. During the DuckDB run the Pi also used up to
    486 MB of swap, compared with 89 MB in the ClickHouse run.</p>""")
    a(h3("7.5", "Temperature"))
    a(table("Table 9. Temperature and throttling", ["Sensors", "Avg. temp, ClickHouse", "Avg. temp, DuckDB", "Max temp, ClickHouse", "Max temp, DuckDB", "Time at soft limit, DuckDB"],
            [["Idle", "54.3 °C", "55.0 °C", "57.9 °C", "59.0 °C", "0%"], ["1", "62.6 °C", "61.4 °C", "66.1 °C", "82.1 °C", "0.3%"],
             ["2", "64.7 °C", "69.7 °C", "67.8 °C", "74.9 °C", "0%"], ["3", "67.3 °C", "75.5 °C", "71.1 °C", "78.2 °C", "0%"],
             ["4", "66.4 °C", "77.6 °C", "71.6 °C", "81.0 °C", "0%"], ["5", "67.5 °C", "80.4 °C", "74.4 °C", "82.1 °C", "1.9%"],
             ["6", "74.4 °C", "81.5 °C", "80.4 °C", "84.8 °C", "48.6%"]],
            note="ClickHouse never reached the soft limit. Both runs started from the same idle temperature."))
    a("""<p>Without cooling, the Pi 5 firmware starts to limit itself at about 80 °C. ClickHouse came close, with a
    peak of 80.4 °C, but never triggered the limit. DuckDB was at the limit for almost half of the time
    with six sensors, and for 84–94% of its last two hours. The Pi only actually lowered its clock speed in
    0.6% of the samples, so the heat itself did not slow DuckDB down much. Both runs started from the same
    idle temperature, which suggests the room was at a similar temperature.</p>""")
    a(h3("7.6", "Storage and disk"))
    a(table("Table 10. Storage after the six-hour run", ["Measure", "ClickHouse", "DuckDB"],
            [["Data on disk", "933 MiB", "3,304 MiB"], ["Of which landing files", "–", "1,503 MiB"],
             ["Of which lake tables", "–", "1,776 MiB"], ["Disk writes with 6 sensors", "1,222 kB/s", "724 kB/s"]], cls="narrow"))
    a("""<p>DuckDB kept every reading twice, once in a landing file and once in a lake table, so it used 3.5
    times the disk space of ClickHouse. ClickHouse wrote more data per second, because it keeps merging
    its parts in the background, but in the end it stored far less.</p>""")

    a(h2("8", "Analysis"))
    a("""<p>The results show what happened. This section tries to explain why, using the same data. It looks
    at where DuckDB’s cycle time went, how fast its delay grew, what each extra sensor cost, how heat
    followed CPU, and how much space each reading needs.</p>""")
    a(h3("8.1", "Where DuckDB’s time went"))
    a("""<p>To understand why DuckDB’s delay kept growing, I split each pipeline cycle into two parts: the time
    spent running the three models, and everything else. The model times come from duckstream’s own log of
    each batch. The cycle time is the gap between one batch and the next. Figure 4 shows both for each
    hour of the run. The first hour is left out because, with one sensor, many cycles found no new file and
    did no work.</p>""")
    a(figure(fig_cycle(), "Figure 4. Average DuckDB cycle time per hour, split into model work and everything else."))
    a("""<p>Running the models took less than a second in every hour. The rest of the cycle, which was 90–92% of
    it, went to other work: listing the landing folder, checking which files were new, and updating the
    record of consumed files. Between hour 4 and hour 6 the load did not change at all, yet the cycle grew
    from 7.8 to 11.6 seconds. The model work grew only from 0.66 to 0.89 seconds, and only because each
    longer cycle had more rows to process. By the end of the run there were 10,180 landing files. This
    points to the file bookkeeping, and not the calculations, as the reason DuckDB slowed down.</p>""")
    a(h3("8.2", "How fast the delay grew"))
    a("""<p>Between 2.5 and 5.5 hours, when all six sensors were running, DuckDB’s median delay rose by about
    1.9 seconds per hour. A straight line fits these points well (R² = 0.84). If that trend simply
    continued, the delay would be around 45 seconds after 24 hours. This is only a rough projection, since
    I did not run the pipeline that long, but it shows that the growth is not a small effect. ClickHouse’s
    delay did not change at all over the same period.</p>""")
    a(h3("8.3", "The cost of each extra sensor"))
    a("""<p>The CPU numbers in Section 7.3 can also be read as the cost of adding one more sensor. For
    ClickHouse, most of the cost came with the first sensor, which woke up the queue consumer. After that,
    each extra sensor added only about 3% of a core. For DuckDB, each extra sensor between one and six
    added about 21% of a core. With six sensors, ClickHouse needed about 40% of a core for every 1,000
    readings per second, while DuckDB needed about 55%. In other words, ClickHouse has a fixed cost that
    does not grow much, while DuckDB’s cost grows steadily with load.</p>""")
    a(h3("8.4", "Heat follows CPU"))
    a("""<p>On a Pi with no cooling, heat is closely tied to CPU. For the DuckDB run, the correlation between
    total CPU and temperature was 0.94, so the temperature followed the CPU almost exactly. For ClickHouse
    it was 0.62, which is weaker because its CPU stayed flat while the board warmed up slowly over the
    hours. The extra 46% of a core that DuckDB used is enough to explain why it reached the soft limit and
    ClickHouse did not.</p>""")
    a(h3("8.5", "Memory and storage per reading"))
    a("""<p>The memory results suggest a trade-off. ClickHouse pays a fixed price of about 1 GB as soon as it
    starts, but then stays flat. DuckDB starts small and grows, and its peak of 1.8 GB would leave very
    little room on a 2 GB board.</p>
    <p>For storage, dividing the disk space by the number of readings gives a simple measure. ClickHouse
    used at most 19 bytes per reading, while DuckDB used about 68 bytes, split mainly between the landing
    file (31 bytes) and the lake table (36 bytes). At 3,000 readings per second, that is roughly 5 GB a day for ClickHouse and 17.5 GB a
    day for DuckDB. On a 64 GB SD card, this difference decides how many days of raw data a device can
    keep.</p>""")

    a(h2("9", "Discussion"))
    a("""<p>Putting the results and the analysis together, ClickHouse was the better choice for this workload.
    It won on six of the nine measures in Table 11, DuckDB won on two, and one was a tie.</p>""")
    a(table("Table 11. Overall comparison", ["Measure", "ClickHouse", "DuckDB", "Better"],
            [["Readings stored", "All, 0 lost", "All, 0 lost", "Tie"],
             ["Delay with 6 sensors (median)", "3 ms", "6.9 s", "ClickHouse"],
             ["Delay trend over 6 hours", "Flat", "Grew to 16.6 s", "ClickHouse"],
             ["CPU with 6 sensors", "119% of a core", "165% of a core", "ClickHouse"],
             ["Memory when idle", "979 MB", "149 MB", "DuckDB"],
             ["Memory at peak", "1,201 MB", "1,843 MB", "ClickHouse"],
             ["Highest temperature", "80.4 °C", "84.8 °C", "ClickHouse"],
             ["Disk used in 6 hours", "933 MiB", "3,304 MiB", "ClickHouse"],
             ["Derived minute stored (median)", "20–600 s", "About 8 s", "DuckDB"]]))
    a(h3("9.1", "Why ClickHouse did better"))
    a("""<p>The main reason is that ClickHouse is pushed data, while duckstream has to go and look for it.
    ClickHouse inserts each batch the moment it arrives, and its materialized views update the summary in
    the same step. duckstream wakes up every two seconds and has to work out which files are new, and
    Section 8.1 showed that this check gets slower as files build up. ClickHouse also writes each reading
    only once, and it lets the dashboard, the ingest and the scheduled jobs run at the same time instead of
    taking turns on a single catalog.</p>""")
    a(h3("9.2", "When DuckDB is still a good choice"))
    a("""<p>DuckDB is not a poor tool; it was simply used for a job it was not designed for. It needs only about
    150 MB when idle, has no server to manage, and keeps its data as plain Parquet files that almost any tool
    can read. With up to three sensors it stored minute results within 3–5 seconds, which is faster than
    the ClickHouse design. For small devices with little memory, or for analysis that runs every few
    minutes instead of all the time, DuckDB remains a sensible choice. Its growing delay also comes from my
    duckstream design rather than from DuckDB itself. Deleting or merging consumed landing files would
    probably keep the cycle short, although I did not test this.</p>""")
    a(h3("9.3", "The effect of running without cooling"))
    a("""<p>Leaving the Pi bare made heat part of the result. A pipeline that uses more CPU on the same board
    produces more heat, and DuckDB’s extra load kept the Pi at its soft limit for most of the last two
    hours. ClickHouse stayed just under the limit, but with a peak of 80.4 °C it had little margin. For a
    device that has to run day and night at this load, either pipeline would need at least a simple
    heatsink.</p>""")

    a(h2("10", "Limitations"))
    a("""<p>This study has some limits that should be kept in mind when reading the results.</p>
    <ul>
    <li><b>One run each.</b> Each pipeline ran once, so there are no repeated trials or confidence intervals.
    The large differences are unlikely to reverse, but smaller ones, such as disk writes, could.</li>
    <li><b>Different days and times.</b> ClickHouse ran overnight (00:54–06:54 Pakistan time) and DuckDB in
    the evening (17:34–23:34). Room temperature was not recorded, although both runs started from the same
    idle temperature.</li>
    <li><b>Minute-level delay.</b> The ClickHouse per-reading audit expired under a 24-hour retention before I
    exported it, so both pipelines are compared using the last reading of each machine-minute. This still
    gives 1,715 samples per run.</li>
    <li><b>Clock offset.</b> Because the laptop clock was not synchronised, the delay measure leaves out
    network and broker time.</li>
    <li><b>Different broker paths.</b> ClickHouse read an AMQP queue, while the DuckDB ingest subscribed over
    MQTT. Both used the same broker and the same sensors.</li>
    <li><b>DuckDB settings.</b> DuckDB ran with its designed settings (10 s file flush, 2 s cycle). A faster
    profile with 1 s for both was prepared but not run.</li>
    <li><b>Simulated data and SD card.</b> The sensors were simulated with no faults, and both pipelines
    stored data on an SD card. An SSD would change any result that depends on the disk.</li>
    </ul>""")

    a(h2("11", "Conclusion and future work"))
    a("""<p>An uncooled Raspberry Pi 5 can receive, store and analyse 3,000 sensor readings per second without
    losing any data, using either DuckDB or ClickHouse. For real-time work, ClickHouse is the better
    choice. It made readings available within milliseconds, its delay stayed flat for six hours, and it
    used less CPU, less disk and produced less heat. The DuckDB pipeline, with the duckstream engine I built
    for it, was complete and correct, but its delay grew with the length of the run because the work of
    finding new files kept increasing. DuckDB is a better fit for devices with little memory and for
    analysis that runs from time to time.</p>
    <p>Several questions are left for future work:</p>
    <ul>
    <li>Repeat each run three or more times to measure how much the results vary.</li>
    <li>Run DuckDB with the faster profile (1 s flush and 1 s cycle).</li>
    <li>Delete or merge consumed landing files in duckstream, and measure the cycle time again.</li>
    <li>Synchronise the clocks so that the full delay from sensor to storage can be measured.</li>
    <li>Repeat both runs with a heatsink to separate the effect of heat.</li>
    <li>Run for 24 hours or more, and with more than six sensors.</li>
    <li>Add bearing and imbalance faults to the simulated signals and compare how well each pipeline
    detects them.</li>
    </ul>""")

    a(h2("12", "References"))
    a("""<ol class="refs">
    <li>DuckDB documentation. https://duckdb.org/docs/</li>
    <li>DuckLake documentation. https://ducklake.select/</li>
    <li>ClickHouse documentation. https://clickhouse.com/docs</li>
    <li>RabbitMQ MQTT plugin documentation. https://www.rabbitmq.com/docs/mqtt</li>
    <li>Raspberry Pi 5 documentation, including temperature and throttling. https://www.raspberrypi.com/documentation/</li>
    </ol>""")

    a(h2("A", "Appendix: data and reproducibility"))
    a("""<p>All data and scripts are kept under <code>engine_pipeline/research/</code> on the Pi.</p>""")
    a(table("Table 12. Data files", ["Location", "Contents"],
            [["runs/ch_run1/", "ClickHouse run: monitoring samples, per-minute audit, latency, per-sensor summary, storage"],
             ["runs/duck_run1/", "DuckDB run: monitoring samples, per-minute audit, per-reading audit (51.3 M rows), batch timings, pipeline log, storage"],
             ["runs/comparison/", "The tables used in this report"],
             ["tools/compare_runs.py", "Rebuilds runs/comparison/ from both runs"],
             ["tools/sysmon.py", "The monitoring script used in both runs"],
             ["report/build_report.py", "Builds this document"]]))
    return "\n".join(b)


CSS = """
@page { size: A4; margin: 24mm 22mm 22mm 22mm;
        @bottom-center { content: counter(page); font: 10pt 'Liberation Serif', serif; color: #555; } }
@page :first { @bottom-center { content: none; } }
body { font-family: 'Liberation Serif', 'Times New Roman', serif; font-size: 11.5pt; line-height: 1.5; color: #111; }
.title-page { height: 245mm; display: flex; flex-direction: column; justify-content: center; align-items: center;
              text-align: center; page-break-after: always; }
.title-page .t { font-size: 26pt; font-weight: bold; line-height: 1.25; margin: 0 10mm; }
.title-page .st { font-size: 15pt; margin-top: 8mm; color: #333; }
.title-page .rule { width: 60mm; border-top: 1px solid #999; margin: 14mm 0; }
.title-page .au { font-size: 15pt; }
.title-page .sv { font-size: 12pt; margin-top: 6mm; }
.title-page .un { font-size: 13pt; margin-top: 3mm; }
.title-page .dt { font-size: 12pt; color: #555; margin-top: 4mm; }
.toc { page-break-after: always; font-size: 10.5pt; line-height: 1.3; }
.toc h1 { font-size: 20pt; margin: 0 0 4mm 0; }
.toc a { color: #111; text-decoration: none; display: flex; align-items: baseline; }
.toc .e2 { font-weight: bold; margin-top: 1.6mm; }
.toc .e3 { padding-left: 9mm; margin-top: 0; }
.toc .lead { flex: 1; border-bottom: 1px dotted #888; margin: 0 2mm; transform: translateY(-3px); }
h2 { font-size: 16pt; margin: 9mm 0 3mm 0; page-break-after: avoid; break-after: avoid; }
h2.newpage { page-break-before: always; }
h3 { font-size: 12.5pt; margin: 6mm 0 2mm 0; page-break-after: avoid; break-after: avoid; }
p { margin: 0 0 3mm 0; text-align: justify; }
ul, ol { margin: 0 0 3mm 0; padding-left: 7mm; }
li { margin-bottom: 1.2mm; }
figure { margin: 5mm 0; break-inside: avoid; page-break-inside: avoid; }
figure.fig svg { width: 100%; height: auto; }
figcaption { font-size: 10pt; color: #333; margin: 2mm 0; }
figure.fig figcaption { text-align: center; }
table { border-collapse: collapse; width: 100%; font-family: 'Liberation Sans', sans-serif; font-size: 9pt; line-height: 1.35; }
figure.narrow table { width: auto; min-width: 60%; }
th { text-align: left; border-bottom: 1.5px solid #333; border-top: 1.5px solid #333; padding: 1.6mm 2mm; background: #f4f4f4; }
td { border-bottom: 1px solid #ddd; padding: 1.4mm 2mm; vertical-align: top; }
.note { font-size: 9pt; color: #555; margin-top: 1.5mm; }
code { font-family: 'Liberation Mono', monospace; font-size: 10pt; }
.refs li { word-break: break-all; }
"""


def build(pages):
    bodyhtml = body()   # fills toc
    entries = []
    for lvl, num, title, anchor in toc:
        pg = pages.get(anchor, "00")   # placeholder keeps contents lines from matching headings
        entries.append(f'<a class="e{lvl}" href="#{anchor}"><span>{num}&nbsp;&nbsp;{html.escape(title)}</span>'
                       f'<span class="lead"></span><span>{pg}</span></a>')
    toc.clear()
    # every top-level section starts on a new page
    bodyhtml = bodyhtml.replace("<h2 ", '<h2 class="newpage" ')
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>{html.escape(TITLE)}</title>
<style>{CSS}</style></head><body>
<section class="title-page"><div class="t">{html.escape(TITLE)}</div><div class="st">{html.escape(SUBTITLE)}</div>
<div class="rule"></div><div class="au">{html.escape(AUTHOR)}</div><div class="sv">Supervisors: {html.escape(SUPERVISORS)}</div><div class="un">{html.escape(UNIVERSITY)}</div><div class="dt">{DATE}</div></section>
<section class="toc"><h1>Contents</h1>{''.join(entries)}</section>
{bodyhtml}
</body></html>"""


def render(html_text, pdf):
    HTML_OUT.write_text(html_text, encoding="utf-8")
    with tempfile.TemporaryDirectory() as prof:
        subprocess.run(["chromium", "--headless", "--no-sandbox", "--disable-gpu", f"--user-data-dir={prof}",
                        "--no-pdf-header-footer", f"--print-to-pdf={pdf}", HTML_OUT.as_uri()],
                       check=True, capture_output=True)


def heading_pages(pdf, skip=None):
    """anchor -> page number, from where each heading's text appears alone on a line.

    Contents pages repeat every heading, so they are skipped: on the first
    pass they are the pages holding the "00" placeholders."""
    n = int(re.search(r"Pages:\s+(\d+)", subprocess.run(["pdfinfo", str(pdf)], capture_output=True, text=True).stdout).group(1))
    body()   # refill toc to know the headings
    heads = [(anchor, f"{num} {title}") for _, num, title, anchor in toc]
    toc.clear()
    found, contents = {}, set()
    for pg in range(1, n + 1):
        t = subprocess.run(["pdftotext", "-f", str(pg), "-l", str(pg), str(pdf), "-"], capture_output=True, text=True).stdout
        lines = {re.sub(r"\s+", " ", l).strip() for l in t.splitlines()}
        if skip is None and "00" in lines:
            contents.add(pg)
        if pg in (skip if skip is not None else contents):
            continue
        for anchor, h in heads:
            if anchor not in found and h in lines:
                found[anchor] = pg
    return found, len(heads), (skip if skip is not None else contents)


def main():
    tmp = HERE / "pass1.pdf"
    render(build({}), tmp)
    pages, total, contents = heading_pages(tmp)
    render(build(pages), PDF_OUT)
    check, _, _ = heading_pages(PDF_OUT, contents)
    tmp.unlink()
    missing = total - len(pages)
    print(f"{PDF_OUT}  headings placed {len(pages)}/{total}" + ("" if check == pages else "  WARNING: page numbers moved on pass 2"))
    if missing:
        print("not found:", missing)


if __name__ == "__main__":
    main()
