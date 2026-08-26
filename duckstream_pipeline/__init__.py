"""The accelerometer pipeline, rebuilt on duckstream.

A *consumer* of duckstream, not part of it. duckstream imports nothing from this
repository and is meant to be extracted to its own; this package is what a user
writes, and it is deliberately small — three model declarations, an SCD2
dimension duckstream does not own, and a loop.
"""
