"""
IoT Data Generator for Databricks IoT Project

This module generates synthetic IoT data for four categories:
- telemetry: Real-time sensor data from IoT devices
- failures: Device failure and error events
- maintenance: Scheduled and unscheduled maintenance records
- device_master: Device metadata and configuration data

The generated data is designed to work with Azure Data Lake Gen2 storage
and Databricks analytics workflows.
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum
import math


class DeviceType(Enum):
    """Supported IoT device types"""

    TEMPERATURE_SENSOR = "temperature_sensor"
    HUMIDITY_SENSOR = "humidity_sensor"
    PRESSURE_SENSOR = "pressure_sensor"
    VIBRATION_SENSOR = "vibration_sensor"
    FLOW_METER = "flow_meter"
    POWER_METER = "power_meter"
    SMART_VALVE = "smart_valve"
    MOTOR_CONTROLLER = "motor_controller"


class DeviceStatus(Enum):
    """Device operational status"""

    ACTIVE = "active"
    INACTIVE = "inactive"
    MAINTENANCE = "maintenance"
    FAILED = "failed"


class FailureType(Enum):
    """Types of device failures"""

    SENSOR_MALFUNCTION = "sensor_malfunction"
    COMMUNICATION_FAILURE = "communication_failure"
    POWER_FAILURE = "power_failure"
    MECHANICAL_FAILURE = "mechanical_failure"
    SOFTWARE_ERROR = "software_error"
    CALIBRATION_DRIFT = "calibration_drift"


class MaintenanceType(Enum):
    """Types of maintenance activities"""

    PREVENTIVE = "preventive"
    CORRECTIVE = "corrective"
    PREDICTIVE = "predictive"
    EMERGENCY = "emergency"


@dataclass
class Device:
    """IoT Device metadata"""

    device_id: str
    device_type: str
    location: str
    facility: str
    installation_date: str
    firmware_version: str
    model: str
    manufacturer: str
    status: str
    last_maintenance: str
    next_scheduled_maintenance: str


@dataclass
class TelemetryReading:
    """Telemetry data point from an IoT device"""

    device_id: str
    timestamp: str
    reading_type: str
    value: float
    unit: str
    quality: str
    location: str
    facility: str


@dataclass
class FailureEvent:
    """Device failure event"""

    event_id: str
    device_id: str
    timestamp: str
    failure_type: str
    severity: str
    description: str
    location: str
    facility: str
    resolved: bool
    resolution_time: Optional[str] = None


@dataclass
class MaintenanceRecord:
    """Device maintenance record"""

    maintenance_id: str
    device_id: str
    maintenance_type: str
    scheduled_date: str
    actual_date: str
    duration_hours: float
    technician_id: str
    description: str
    parts_replaced: List[str]
    cost: float
    location: str
    facility: str


class IoTDataGenerator:
    """Generates synthetic IoT data for multiple data categories"""

    def __init__(self, config=None):
        """Initialize the IoT data generator with configuration"""
        if config is None:
            from .config import CONFIG

            config = CONFIG

        self.config = config
        self.num_devices = config["devices"]["num_devices"]
        self.devices: List[Device] = []

        # Load configuration data
        sim_config = config["simulation"]
        self.facilities = sim_config["facilities"]
        self.locations = sim_config["locations"]
        self.manufacturers = sim_config["manufacturers"]
        self.device_status_weights = sim_config["device_status_weights"]

        # Initialize devices
        self._generate_device_master_data()

    def _generate_device_master_data(self) -> None:
        """Generate master data for all devices"""
        for i in range(self.num_devices):
            device_type = random.choice(list(DeviceType))
            facility = random.choice(self.facilities)
            location = random.choice(self.locations[facility])

            installation_date = self._random_date_in_past(
                days=365 * 3
            )  # Up to 3 years ago
            last_maintenance = self._random_date_between(
                installation_date, datetime.now()
            )
            next_maintenance = self._random_date_in_future(days=90)  # Next 3 months

            device = Device(
                device_id=f"DEV_{device_type.value.upper()}_{i:04d}",
                device_type=device_type.value,
                location=location,
                facility=facility,
                installation_date=installation_date.isoformat(),
                firmware_version=f"{random.randint(1,5)}.{random.randint(0,9)}.{random.randint(0,9)}",
                model=f"{device_type.value.upper()}-{random.randint(100,999)}",
                manufacturer=random.choice(self.manufacturers),
                status=random.choices(
                    list(self.device_status_weights.keys()),
                    weights=list(self.device_status_weights.values()),
                )[0],
                last_maintenance=last_maintenance.isoformat(),
                next_scheduled_maintenance=next_maintenance.isoformat(),
            )
            self.devices.append(device)

    def _random_date_in_past(self, days: int) -> datetime:
        """Generate a random date in the past"""
        return datetime.now() - timedelta(days=random.randint(1, days))

    def _random_date_in_future(self, days: int) -> datetime:
        """Generate a random date in the future"""
        return datetime.now() + timedelta(days=random.randint(1, days))

    def _random_date_between(
        self, start_date: datetime, end_date: datetime
    ) -> datetime:
        """Generate a random date between two dates"""
        time_between = end_date - start_date
        days_between = time_between.days
        random_days = random.randint(0, days_between)
        return start_date + timedelta(days=random_days)

    def _get_sensor_reading_config(self, device_type: str) -> Dict[str, Any]:
        """Get sensor reading configuration for different device types"""
        sensors = self.config.get("sensors", {})
        return sensors.get(
            device_type,
            sensors.get(
                "temperature_sensor",
                {
                    "reading_type": "temperature",
                    "unit": "celsius",
                    "base_value": 25.0,
                    "variance": 15.0,
                    "min_val": -10.0,
                    "max_val": 80.0,
                },
            ),
        )

    def generate_telemetry_data(
        self, hours: int = 24, interval_minutes: int = 5
    ) -> List[Dict[str, Any]]:
        """Generate telemetry data for specified time period"""
        telemetry_data = []

        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        current_time = start_time

        # Only generate data for active devices
        active_devices = [d for d in self.devices if d.status == "active"]

        while current_time <= end_time:
            for device in active_devices:
                # Get data quality configuration
                quality_config = self.config.get("data_quality", {})
                missing_prob = quality_config.get("missing_reading_probability", 0.05)

                # Some devices might miss readings (simulate network issues)
                if random.random() > (1 - missing_prob):
                    continue

                config = self._get_sensor_reading_config(device.device_type)

                # Generate realistic sensor reading with some drift and noise
                base_value = config["base_value"]
                variance = config["variance"]

                # Add time-based patterns (e.g., temperature cycles)
                time_factor = math.sin(2 * math.pi * current_time.hour / 24)
                seasonal_drift = time_factor * variance * 0.3

                # Add random noise
                noise = random.gauss(0, variance * 0.1)

                value = base_value + seasonal_drift + noise
                value = max(config["min_val"], min(config["max_val"], value))

                # Quality indicator
                quality_weights = quality_config.get(
                    "quality_weights", {"good": 0.9, "fair": 0.08, "poor": 0.02}
                )
                quality = random.choices(
                    list(quality_weights.keys()),
                    weights=list(quality_weights.values()),
                )[0]

                reading = TelemetryReading(
                    device_id=device.device_id,
                    timestamp=current_time.isoformat(),
                    reading_type=config["reading_type"],
                    value=round(value, 2),
                    unit=config["unit"],
                    quality=quality,
                    location=device.location,
                    facility=device.facility,
                )

                telemetry_data.append(asdict(reading))

            current_time += timedelta(minutes=interval_minutes)

        return telemetry_data

    def generate_failure_events(self, days: int = 30) -> List[Dict[str, Any]]:
        """Generate failure events for specified time period"""
        failure_events = []

        # Get failure configuration
        failure_config = self.config.get("failures", {})
        mtbf_days = failure_config.get("mtbf_days", 90)

        # Calculate expected number of failures based on device count and time
        # Assume average MTBF (Mean Time Between Failures) from configuration
        expected_failures = max(1, int(self.num_devices * days / mtbf_days))

        for _ in range(expected_failures):
            device = random.choice(self.devices)
            failure_type = random.choice(list(FailureType))

            # Failure timestamp in the last 'days' period
            failure_time = self._random_date_in_past(days)

            # Get severity weights from configuration
            failure_config = self.config.get("failures", {})
            severity_weights_config = failure_config.get("severity_weights", {})

            # Severity based on failure type
            default_weights = [0.6, 0.3, 0.1]  # [low, medium, high]
            failure_type_config = severity_weights_config.get(failure_type.value, {})

            if failure_type_config:
                weights = [
                    failure_type_config.get("low", 0.6),
                    failure_type_config.get("medium", 0.3),
                    failure_type_config.get("high", 0.1),
                ]
            else:
                weights = default_weights

            severity = random.choices(["low", "medium", "high"], weights=weights)[0]

            # Resolution status and time
            resolution_probability = failure_config.get("resolution_probability", 0.85)
            resolved = random.choices(
                [True, False],
                weights=[resolution_probability, 1 - resolution_probability],
            )[0]
            resolution_time = None
            if resolved:
                # Resolution time based on severity from configuration
                resolution_config = failure_config.get("resolution_time_hours", {})
                severity_config = resolution_config.get(severity, {"min": 1, "max": 8})
                resolution_hours = random.randint(
                    severity_config.get("min", 1), severity_config.get("max", 8)
                )
                resolution_time = (
                    failure_time + timedelta(hours=resolution_hours)
                ).isoformat()

            descriptions = {
                FailureType.SENSOR_MALFUNCTION: "Sensor reading outside normal range",
                FailureType.COMMUNICATION_FAILURE: "Device lost network connectivity",
                FailureType.POWER_FAILURE: "Power supply interruption detected",
                FailureType.MECHANICAL_FAILURE: "Mechanical component failure",
                FailureType.SOFTWARE_ERROR: "Software exception occurred",
                FailureType.CALIBRATION_DRIFT: "Sensor calibration outside tolerance",
            }

            failure = FailureEvent(
                event_id=str(uuid.uuid4()),
                device_id=device.device_id,
                timestamp=failure_time.isoformat(),
                failure_type=failure_type.value,
                severity=severity,
                description=descriptions[failure_type],
                location=device.location,
                facility=device.facility,
                resolved=resolved,
                resolution_time=resolution_time,
            )

            failure_events.append(asdict(failure))

        return failure_events

    def generate_maintenance_records(self, days: int = 90) -> List[Dict[str, Any]]:
        """Generate maintenance records for specified time period"""
        maintenance_records = []

        # Get maintenance configuration
        maintenance_config = self.config.get("maintenance", {})
        maintenance_probability = maintenance_config.get("maintenance_probability", 0.7)

        # Generate scheduled maintenance for devices
        for device in self.devices:
            # Most devices should have at least one maintenance in the specified period
            if random.random() < maintenance_probability:
                # Get maintenance type weights from configuration
                type_weights_config = maintenance_config.get("type_weights", {})
                maintenance_types = (
                    list(type_weights_config.keys())
                    if type_weights_config
                    else [t.value for t in MaintenanceType]
                )
                weights = (
                    list(type_weights_config.values())
                    if type_weights_config
                    else [0.6, 0.2, 0.15, 0.05]
                )

                maintenance_type = random.choices(maintenance_types, weights=weights)[0]

                # Scheduled vs actual date variance
                scheduled_date = self._random_date_in_past(days)
                if maintenance_type == "emergency":
                    actual_date = (
                        scheduled_date  # Emergency maintenance happens immediately
                    )
                else:
                    # Small variance between scheduled and actual
                    variance_hours = random.randint(-24, 48)  # -1 day to +2 days
                    actual_date = scheduled_date + timedelta(hours=variance_hours)

                # Duration based on maintenance type from configuration
                duration_config = maintenance_config.get("duration_hours", {})
                type_duration = duration_config.get(
                    maintenance_type, {"min": 1.0, "max": 4.0}
                )
                min_duration = type_duration.get("min", 1.0)
                max_duration = type_duration.get("max", 4.0)
                duration = round(random.uniform(min_duration, max_duration), 1)

                # Parts that might be replaced from configuration
                all_parts = maintenance_config.get(
                    "available_parts",
                    [
                        "sensor_calibration",
                        "filter_replacement",
                        "cable_connector",
                        "battery_backup",
                        "firmware_update",
                        "mounting_bracket",
                        "protective_housing",
                        "communication_module",
                    ],
                )

                # Number of parts replaced based on configuration weights
                parts_count_weights = maintenance_config.get(
                    "parts_count_weights", {0: 0.4, 1: 0.4, 2: 0.15, 3: 0.05}
                )
                num_parts = random.choices(
                    list(parts_count_weights.keys()),
                    weights=list(parts_count_weights.values()),
                )[0]
                parts_replaced = (
                    random.sample(all_parts, min(num_parts, len(all_parts)))
                    if num_parts > 0
                    else []
                )

                # Cost based on duration and parts from configuration
                labor_cost_per_hour = maintenance_config.get(
                    "labor_cost_per_hour", 75.0
                )
                parts_cost_range = maintenance_config.get(
                    "parts_cost_range", {"min": 50.0, "max": 200.0}
                )

                base_cost = duration * labor_cost_per_hour
                parts_cost = len(parts_replaced) * random.uniform(
                    parts_cost_range.get("min", 50.0),
                    parts_cost_range.get("max", 200.0),
                )
                total_cost = round(base_cost + parts_cost, 2)

                descriptions = {
                    "preventive": "Routine preventive maintenance check",
                    "corrective": "Corrective maintenance to address identified issues",
                    "predictive": "Predictive maintenance based on sensor data analysis",
                    "emergency": "Emergency repair due to critical failure",
                }

                maintenance = MaintenanceRecord(
                    maintenance_id=str(uuid.uuid4()),
                    device_id=device.device_id,
                    maintenance_type=maintenance_type,
                    scheduled_date=scheduled_date.isoformat(),
                    actual_date=actual_date.isoformat(),
                    duration_hours=duration,
                    technician_id=f"TECH_{random.randint(1001, 1050)}",
                    description=descriptions[maintenance_type],
                    parts_replaced=parts_replaced,
                    cost=total_cost,
                    location=device.location,
                    facility=device.facility,
                )

                maintenance_records.append(asdict(maintenance))

        return maintenance_records

    def get_device_master_data(self) -> List[Dict[str, Any]]:
        """Get device master data"""
        return [asdict(device) for device in self.devices]

    def generate_all_data(
        self,
        telemetry_hours: int = 24,
        failure_days: int = 30,
        maintenance_days: int = 90,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Generate all types of data"""
        return {
            "telemetry": self.generate_telemetry_data(telemetry_hours),
            "failures": self.generate_failure_events(failure_days),
            "maintenance": self.generate_maintenance_records(maintenance_days),
            "device_master": self.get_device_master_data(),
        }

    def save_data_to_json(
        self, data: Dict[str, List[Dict[str, Any]]], output_dir: str = "./output"
    ) -> Dict[str, str]:
        """Save generated data to JSON files"""
        import os

        os.makedirs(output_dir, exist_ok=True)

        file_paths = {}
        for data_type, records in data.items():
            file_path = os.path.join(output_dir, f"{data_type}.json")
            with open(file_path, "w") as f:
                json.dump(records, f, indent=2)
            file_paths[data_type] = file_path
            print(f"Saved {len(records)} {data_type} records to {file_path}")

        return file_paths


def main():
    """Main function to demonstrate the data generator"""
    print("IoT Data Generator Demo")
    print("=" * 50)

    # Load configuration
    from .config import load_config

    config = load_config()

    # Override for demo
    config.num_devices = 50

    # Initialize generator with configuration
    generator = IoTDataGenerator(config=config)

    print(f"Generated {len(generator.devices)} devices")

    # Generate all data types
    all_data = generator.generate_all_data(
        telemetry_hours=48,  # 48 hours of telemetry data
        failure_days=30,  # 30 days of failure events
        maintenance_days=90,  # 90 days of maintenance records
    )

    # Print summary statistics
    for data_type, records in all_data.items():
        print(f"{data_type.capitalize()}: {len(records)} records")

    # Save to files
    file_paths = generator.save_data_to_json(all_data, config.output_directory)

    print("\nFiles saved:")
    for data_type, path in file_paths.items():
        print(f"  {data_type}: {path}")


if __name__ == "__main__":
    main()
