#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "altair>=5.0.0",
#   "vl-convert-python>=1.0.0",
#   "pandas>=2.0.0",
# ]
# ///
import json
import sys

try:
    from vl_convert import vegalite_to_png
except Exception as exc:
    sys.stderr.write(f"Missing vl-convert-python: {exc}\n")
    sys.exit(1)

spec = json.load(sys.stdin)

png_data = vegalite_to_png(spec)
sys.stdout.buffer.write(png_data)
