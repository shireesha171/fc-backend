"""Tests for fileops-ValidationProcess lambda"""

from ..app import lambda_handler
from .utils import import_file as f

import json
from pathlib import Path

ROOT_FOLDER = Path.cwd().parents[1]

event_str = f.import_file(f'{ROOT_FOLDER}/events/', 'fileopsDataProfiling.json')
event = json.loads(event_str)

try:
    result = lambda_handler(event, None)
    print(result)
except Exception as e:
    print(f"Error: {e.with_traceback()}")
