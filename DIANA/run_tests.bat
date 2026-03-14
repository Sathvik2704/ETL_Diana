@echo off
REM Convenience wrapper to run pytest even if Scripts is not on PATH.
REM Uses the current Python interpreter.

python -m pytest %*

