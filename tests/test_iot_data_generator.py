"""
Tests for the IoT data generator
"""

import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import json
import tempfile
from datetime import datetime
from pathlib import Path

from data_simulation.iot_data_generator import (
    Device,
    DeviceStatus,
    DeviceType,
    FailureType,
    IoTDataGenerator,
    MaintenanceType,
    TelemetryReading,
)


def test_device_type_enum():
    """Test DeviceType enum values"""
    assert DeviceType.TEMPERATURE_SENSOR.value == "temperature_sensor"
    assert DeviceType.HUMIDITY_SENSOR.value == "humidity_sensor"
    assert DeviceType.PRESSURE_SENSOR.value == "pressure_sensor"
    assert DeviceType.VIBRATION_SENSOR.value == "vibration_sensor"
    assert DeviceType.FLOW_METER.value == "flow_meter"
    assert DeviceType.POWER_METER.value == "power_meter"
    assert DeviceType.SMART_VALVE.value == "smart_valve"
    assert DeviceType.MOTOR_CONTROLLER.value == "motor_controller"


def test_device_status_enum():
    """Test DeviceStatus enum values"""
    assert DeviceStatus.ACTIVE.value == "active"
    assert DeviceStatus.INACTIVE.value == "inactive"
    assert DeviceStatus.MAINTENANCE.value == "maintenance"
    assert DeviceStatus.FAILED.value == "failed"


def test_failure_type_enum():
    """Test FailureType enum values"""
    assert FailureType.SENSOR_MALFUNCTION.value == "sensor_malfunction"
    assert FailureType.COMMUNICATION_FAILURE.value == "communication_failure"
    assert FailureType.POWER_FAILURE.value == "power_failure"
    assert FailureType.MECHANICAL_FAILURE.value == "mechanical_failure"
    assert FailureType.SOFTWARE_ERROR.value == "software_error"
    assert FailureType.CALIBRATION_DRIFT.value == "calibration_drift"


def test_maintenance_type_enum():
    """Test MaintenanceType enum values"""
    assert MaintenanceType.PREVENTIVE.value == "preventive"
    assert MaintenanceType.CORRECTIVE.value == "corrective"
    assert MaintenanceType.PREDICTIVE.value == "predictive"
    assert MaintenanceType.EMERGENCY.value == "emergency"


def test_device_dataclass():
    """Test Device dataclass"""
    device = Device(
        device_id="TEST_001",
        device_type="temperature_sensor",
        location="Test_Location",
        facility="Test_Facility",
        installation_date="2023-01-01T00:00:00",
        firmware_version="1.0.0",
        model="TEST-100",
        manufacturer="TestCorp",
        status="active",
        last_maintenance="2023-06-01T00:00:00",
        next_scheduled_maintenance="2023-12-01T00:00:00",
    )

    assert device.device_id == "TEST_001"
    assert device.device_type == "temperature_sensor"
    assert device.status == "active"


def test_telemetry_reading_dataclass():
    """Test TelemetryReading dataclass"""
    reading = TelemetryReading(
        device_id="TEST_001",
        timestamp="2023-10-13T10:00:00",
        reading_type="temperature",
        value=25.5,
        unit="celsius",
        quality="good",
        location="Test_Location",
        facility="Test_Facility",
    )

    assert reading.device_id == "TEST_001"
    assert reading.value == 25.5
    assert reading.quality == "good"


def test_iot_data_generator_init():
    """Test generator initialization with default configuration"""
    generator = IoTDataGenerator()

    assert generator.num_devices > 0
    assert len(generator.devices) == generator.num_devices
    assert len(generator.facilities) > 0
    assert len(generator.manufacturers) > 0


def test_device_generation():
    """Test device master data generation"""
    generator = IoTDataGenerator()

    for device in generator.devices[:5]:  # Test first 5 devices
        assert device.device_id.startswith("DEV_")
        assert device.device_type in [dt.value for dt in DeviceType]
        assert device.status in [ds.value for ds in DeviceStatus]
        assert device.facility in generator.facilities
        assert device.manufacturer in generator.manufacturers

        # Test date formatting
        datetime.fromisoformat(device.installation_date)
        datetime.fromisoformat(device.last_maintenance)
        datetime.fromisoformat(device.next_scheduled_maintenance)


def test_generate_all_data():
    """Test generating all data types"""
    generator = IoTDataGenerator()

    all_data = generator.generate_all_data(
        telemetry_hours=1, failure_days=7, maintenance_days=30
    )

    assert isinstance(all_data, dict)
    assert "telemetry" in all_data
    assert "failures" in all_data
    assert "maintenance" in all_data
    assert "device_master" in all_data

    assert isinstance(all_data["telemetry"], list)
    assert isinstance(all_data["failures"], list)
    assert isinstance(all_data["maintenance"], list)
    assert isinstance(all_data["device_master"], list)


def test_save_data_to_json():
    """Test saving data to JSON files"""
    generator = IoTDataGenerator()

    test_data = {
        "telemetry": [{"device_id": "TEST_001", "value": 25.0}],
        "failures": [{"event_id": "uuid-123", "device_id": "TEST_001"}],
        "maintenance": [{"maintenance_id": "uuid-456", "device_id": "TEST_001"}],
        "device_master": [{"device_id": "TEST_001", "status": "active"}],
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        file_paths = generator.save_data_to_json(test_data, temp_dir)

        assert len(file_paths) == 4
        assert all(data_type in file_paths for data_type in test_data.keys())

        # Verify files were created and contain valid JSON
        for data_type, file_path in file_paths.items():
            assert Path(file_path).exists()
            with open(file_path, "r") as f:
                saved_data = json.load(f)
                assert saved_data == test_data[data_type]
