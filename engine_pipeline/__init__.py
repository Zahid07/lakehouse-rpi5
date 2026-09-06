"""The engine vibration pipeline, on duckstream.

A *consumer* of duckstream, not part of it. The package imports nothing from
here and is meant to be extracted to its own repository.

Independent of `duckstream_pipeline/` in every respect that matters: its own
`ENG_*` environment, its own catalog, its own MQTT topic and its own dashboard
port. The two can run side by side, though on a 4 GB Pi they are meant to be run
one at a time -- see the README.
"""
