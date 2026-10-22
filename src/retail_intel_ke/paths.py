import os
from pathlib import Path

PROJECT_ROOT = Path(os.environ.get("RETAIL_INTEL_ROOT", Path(__file__).resolve().parents[2]))

def resolve(p):
    """Relative paths are taken from the project root; absolute paths pass through."""
    p = Path(p)
    return p if p.is_absolute() else PROJECT_ROOT / p
    