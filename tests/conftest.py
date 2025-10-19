"""
Pytest configuration and fixtures for IoT Data Generator tests
"""

import os
import sys
import tempfile
from pathlib import Path
import pytest

# Add the src directory to the Python path for testing
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@pytest.fixture
def temp_directory():
    """Create a temporary directory for test outputs"""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def test_config_file():
    """Provide path to test configuration file"""
    return Path(__file__).parent / "test_config.yaml"


@pytest.fixture
def sample_iot_data():
    """Provide sample IoT data for testing"""
    return {
        "telemetry": [
            {
                "device_id": "TEST_TEMP_001",
                "timestamp": "2023-10-13T10:00:00",
                "reading_type": "temperature",
                "value": 25.5,
                "unit": "celsius",
                "quality": "good",
                "location": "Test_Line_1",
                "facility": "Test_Factory",
            },
            {
                "device_id": "TEST_HUM_001",
                "timestamp": "2023-10-13T10:00:00",
                "reading_type": "humidity",
                "value": 45.2,
                "unit": "percent",
                "quality": "good",
                "location": "Test_Line_1",
                "facility": "Test_Factory",
            },
        ],
        "failures": [
            {
                "event_id": "test-uuid-123",
                "device_id": "TEST_TEMP_001",
                "timestamp": "2023-10-13T08:30:00",
                "failure_type": "sensor_malfunction",
                "severity": "medium",
                "description": "Sensor reading outside normal range",
                "location": "Test_Line_1",
                "facility": "Test_Factory",
                "resolved": True,
                "resolution_time": "2023-10-13T10:15:00",
            }
        ],
        "maintenance": [
            {
                "maintenance_id": "test-uuid-456",
                "device_id": "TEST_TEMP_001",
                "maintenance_type": "preventive",
                "scheduled_date": "2023-10-12T09:00:00",
                "actual_date": "2023-10-12T09:15:00",
                "duration_hours": 1.5,
                "technician_id": "TECH_TEST_001",
                "description": "Routine preventive maintenance check",
                "parts_replaced": ["sensor_calibration"],
                "cost": 125.0,
                "location": "Test_Line_1",
                "facility": "Test_Factory",
            }
        ],
        "device_master": [
            {
                "device_id": "TEST_TEMP_001",
                "device_type": "temperature_sensor",
                "location": "Test_Line_1",
                "facility": "Test_Factory",
                "installation_date": "2023-01-15T00:00:00",
                "firmware_version": "1.2.3",
                "model": "TEMP-SENSOR-100",
                "manufacturer": "TestCorp",
                "status": "active",
                "last_maintenance": "2023-10-12T09:15:00",
                "next_scheduled_maintenance": "2024-01-12T09:00:00",
            },
            {
                "device_id": "TEST_HUM_001",
                "device_type": "humidity_sensor",
                "location": "Test_Line_1",
                "facility": "Test_Factory",
                "installation_date": "2023-01-20T00:00:00",
                "firmware_version": "2.1.0",
                "model": "HUM-SENSOR-200",
                "manufacturer": "MockDevices",
                "status": "active",
                "last_maintenance": "2023-09-20T10:00:00",
                "next_scheduled_maintenance": "2023-12-20T10:00:00",
            },
        ],
    }


@pytest.fixture
def minimal_config():
    """Provide minimal configuration for testing"""
    from data_simulation.config import DataGeneratorConfig

    return DataGeneratorConfig(
        num_devices=5,
        telemetry_hours=1,
        failure_days=1,
        maintenance_days=1,
        telemetry_interval_minutes=30,
        output_directory="./test_output",
    )


@pytest.fixture(autouse=True)
def clean_environment():
    """Clean up environment variables before/after each test"""
    # Store original values
    original_env = {}
    env_vars_to_clean = [
        "IOT_NUM_DEVICES",
        "IOT_TELEMETRY_HOURS",
        "IOT_FAILURE_DAYS",
        "IOT_MAINTENANCE_DAYS",
        "IOT_TELEMETRY_INTERVAL",
        "IOT_OUTPUT_FORMAT",
        "IOT_OUTPUT_DIR",
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_STORAGE_ACCOUNT_KEY",
        "AZURE_CONTAINER_NAME",
    ]

    for var in env_vars_to_clean:
        if var in os.environ:
            original_env[var] = os.environ[var]
            del os.environ[var]

    yield

    # Restore original values
    for var in env_vars_to_clean:
        if var in os.environ:
            del os.environ[var]
        if var in original_env:
            os.environ[var] = original_env[var]


@pytest.fixture
def mock_azure_dependencies():
    """Mock Azure dependencies that might not be installed"""
    import sys
    from unittest.mock import MagicMock

    # Mock Azure modules
    azure_modules = [
        "azure",
        "azure.storage",
        "azure.storage.filedatalake",
        "azure.identity",
    ]

    mocked_modules = {}
    for module_name in azure_modules:
        if module_name not in sys.modules:
            mocked_modules[module_name] = MagicMock()
            sys.modules[module_name] = mocked_modules[module_name]

    yield mocked_modules

    # Clean up mocked modules
    for module_name in mocked_modules:
        if module_name in sys.modules:
            del sys.modules[module_name]


# Configure pytest markers
def pytest_configure(config):
    """Configure custom pytest markers"""
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line(
        "markers", "azure: mark test as requiring Azure dependencies"
    )
    config.addinivalue_line("markers", "slow: mark test as slow running")


# Skip tests based on conditions
def pytest_collection_modifyitems(config, items):
    """Modify test collection based on conditions"""
    # Skip Azure tests if dependencies are not available
    try:
        import azure.storage.filedatalake
        import azure.identity

        azure_available = True
    except ImportError:
        azure_available = False

    if not azure_available:
        azure_marker = pytest.mark.skip(reason="Azure dependencies not available")
        for item in items:
            if "azure" in item.keywords:
                item.add_marker(azure_marker)
