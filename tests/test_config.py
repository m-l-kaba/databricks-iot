"""Tests for the configuration module"""

import tempfile
import yaml
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from data_simulation.config import load_config


def test_load_config_with_valid_file():
    """Test loading configuration from a valid YAML file"""
    config_data = {
        "devices": {"num_devices": 50},
        "data_periods": {
            "telemetry_hours": 12,
            "failure_days": 15,
            "maintenance_days": 45,
        },
        "telemetry": {"interval_minutes": 10},
        "sensors": {
            "temperature_sensor": {
                "reading_type": "temperature",
                "unit": "celsius",
                "base_value": 20.0,
                "variance": 5.0,
                "min_val": 0.0,
                "max_val": 40.0,
            }
        },
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(config_data, f)
        temp_file = f.name

    try:
        config = load_config(temp_file)

        assert config["devices"]["num_devices"] == 50
        assert config["data_periods"]["telemetry_hours"] == 12
        assert config["sensors"]["temperature_sensor"]["base_value"] == 20.0

    finally:
        Path(temp_file).unlink()


def test_load_config_file_not_found():
    """Test error when config file doesn't exist"""
    try:
        load_config("/nonexistent/path.yaml")
        assert False, "Should have raised FileNotFoundError"
    except FileNotFoundError:
        pass  # Expected


def test_load_config_invalid_yaml():
    """Test loading invalid YAML file"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("invalid: yaml: content: [")
        temp_file = f.name

    try:
        config = load_config(temp_file)
        assert False, "Should have raised YAML error"
    except:
        pass  # Expected

    finally:
        Path(temp_file).unlink()


def test_load_default_config():
    """Test loading default config file"""
    # This should load the actual config.yaml from the project
    config = load_config()

    assert isinstance(config, dict)
    assert "devices" in config
    assert "data_periods" in config
    assert "sensors" in config
