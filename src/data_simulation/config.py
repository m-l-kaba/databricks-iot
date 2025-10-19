"""Configuration settings for IoT Data Generator"""

import yaml
from pathlib import Path
from typing import Dict, Any


def load_config(config_path: str = None) -> Dict[str, Any]:
    """Load configuration from YAML config file."""
    if config_path is None:
        current_dir = Path(__file__).parent
        config_path = current_dir.parent.parent / "config.yaml"

    if not Path(config_path).exists():
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, "r") as f:
        config_data = yaml.safe_load(f)

    return config_data


CONFIG = load_config()
