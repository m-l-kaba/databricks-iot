"""IoT Data Generator for Databricks IoT Project

Generates synthetic IoT data for four categories:
- telemetry: Real-time sensor data from IoT devices
- failures: Device failure and error events
- maintenance: Scheduled and unscheduled maintenance records
- device_master: Device metadata and configuration data
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
    temperature: float
    pressure: float
    vibration: float
    humidity: float
    power_consumption: float
    flow_rate: float
    valve_position: float
    motor_speed: float
    location: str
    facility: str


@dataclass
class FailureEvent:
    """Device failure event"""

    failure_id: str
    device_id: str
    failure_timestamp: str
    repair_timestamp: str
    failure_type: str
    severity: str
    root_cause: str
    repair_action: str
    cost: float


@dataclass
class MaintenanceRecord:
    """Device maintenance record"""

    maintenance_id: str
    device_id: str
    maintenance_type: str
    scheduled_date: str
    maintenance_date: str
    duration_minutes: int
    technician: str
    description: str
    cost: float
    parts_replaced: str


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
        """Generate master data for all devices with coherent relationships"""
        for i in range(self.num_devices):
            device_type = random.choice(list(DeviceType))
            facility = random.choice(self.facilities)
            location = random.choice(self.locations[facility])

            # Ensure temporal coherence: installation -> last_maintenance -> now -> next_maintenance
            installation_date = self._random_date_in_past(
                days=365 * 3
            )  # Up to 3 years ago

            # Last maintenance must be after installation but before now
            last_maintenance = self._random_date_between(
                installation_date, datetime.now() - timedelta(days=1)
            )

            # Next maintenance should be in the future, but some devices may be overdue
            if random.random() < 0.15:  # 15% of devices overdue for maintenance
                next_maintenance = self._random_date_between(
                    last_maintenance, datetime.now() - timedelta(days=1)
                )
            else:
                next_maintenance = self._random_date_in_future(days=90)  # Next 3 months

            # Use consistent device ID format for all devices
            device_id = f"DEV_{str(i + 1).zfill(3)}"  # DEV_001, DEV_002, etc.

            device = Device(
                device_id=device_id,
                device_type=device_type.value,
                location=location,
                facility=facility,
                installation_date=installation_date.isoformat()
                + ".000000",  # Match expected format
                firmware_version=f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
                model=f"{device_type.value.upper()}-{random.randint(100, 999)}",
                manufacturer=random.choice(self.manufacturers),
                status=random.choices(
                    list(self.device_status_weights.keys()),
                    weights=list(self.device_status_weights.values()),
                )[0],
                last_maintenance=last_maintenance.isoformat() + ".000000",
                next_scheduled_maintenance=next_maintenance.isoformat() + ".000000",
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

        # Use a coordinated timeline that enables predictive maintenance
        # Generate historical telemetry data that aligns with failure prediction windows
        end_time = datetime.now() - timedelta(
            hours=1
        )  # End 1 hour ago for better alignment
        start_time = end_time - timedelta(hours=hours)
        current_time = start_time

        active_devices = [d for d in self.devices if d.status == "active"]

        while current_time <= end_time:
            for device in active_devices:
                quality_config = self.config.get("data_quality", {})
                missing_prob = quality_config.get("missing_reading_probability", 0.05)

                if random.random() > (1 - missing_prob):
                    continue

                # Generate realistic sensor readings with time-based patterns
                time_factor = math.sin(2 * math.pi * current_time.hour / 24)

                # Temperature (20-90°C)
                temp_base = 60.0
                temperature = temp_base + time_factor * 15 + random.gauss(0, 5)
                temperature = max(20, min(90, temperature))

                # Pressure (800-1200 PSI)
                pressure_base = 1000.0
                pressure = pressure_base + random.gauss(0, 100)
                pressure = max(800, min(1200, pressure))

                # Vibration (2-15 Hz)
                vibration_base = 8.0
                vibration = vibration_base + random.gauss(0, 2)
                vibration = max(2, min(15, vibration))

                # Humidity (30-70%)
                humidity_base = 50.0
                humidity = humidity_base + time_factor * 10 + random.gauss(0, 5)
                humidity = max(30, min(70, humidity))

                # Power consumption (100-600W)
                power_base = 350.0
                power_consumption = power_base + random.gauss(0, 50)
                power_consumption = max(100, min(600, power_consumption))

                # Flow rate (10-50 L/min)
                flow_rate_base = 30.0
                flow_rate = flow_rate_base + random.gauss(0, 5)
                flow_rate = max(10, min(50, flow_rate))

                # Motor speed (1000-3000 RPM)
                motor_speed_base = 2000.0
                motor_speed = motor_speed_base + random.gauss(0, 200)
                motor_speed = max(1000, min(3000, motor_speed))

                valve_position = random.uniform(0, 100)

                reading = TelemetryReading(
                    device_id=device.device_id,
                    timestamp=current_time.isoformat() + ".000000",
                    temperature=round(temperature, 2),
                    pressure=round(pressure, 2),
                    vibration=round(vibration, 2),
                    humidity=round(humidity, 2),
                    power_consumption=round(power_consumption, 2),
                    flow_rate=round(flow_rate, 2),
                    valve_position=round(valve_position, 2),
                    motor_speed=round(motor_speed, 2),
                    location=device.location,
                    facility=device.facility,
                )

                telemetry_data.append(asdict(reading))

            current_time += timedelta(minutes=interval_minutes)

        return telemetry_data

    def generate_failure_events(self, days: int = 30) -> List[Dict[str, Any]]:
        """Generate failure events for specified time period with proper coherence"""
        failure_events = []

        # Get failure configuration
        failure_config = self.config.get("failures", {})
        mtbf_days = failure_config.get("mtbf_days", 90)

        # Calculate expected number of failures based on device count and time
        # Increase failure rate for better ML training data
        expected_failures = max(
            20, int(self.num_devices * days / mtbf_days * 2.5)
        )  # 2.5x multiplier for more failures

        # Only generate failures for active devices (coherence check)
        active_devices = [
            d for d in self.devices if d.status in ["active", "maintenance"]
        ]

        # Generate failures with temporal distribution for better training
        for i in range(expected_failures):
            if not active_devices:
                break

            device = random.choice(active_devices)

            # Distribute failures across the time period (not just last 3 days)
            # This ensures we have historical failures for training
            failure_days_back = random.randint(1, days)
            failure_time = datetime.now() - timedelta(
                days=failure_days_back,
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            )

            # Generate severity with better distribution for ML training
            severity = random.choices(
                ["minor", "major", "critical"],
                weights=[0.4, 0.4, 0.2],  # More balanced distribution
            )[0]

            # Repair time based on severity
            if severity == "critical":
                repair_hours = random.randint(8, 72)
            elif severity == "major":
                repair_hours = random.randint(4, 36)
            else:
                repair_hours = random.randint(1, 12)

            repair_time = failure_time + timedelta(hours=repair_hours)

            # Cost based on severity
            if severity == "critical":
                cost = random.uniform(3000, 15000)
            elif severity == "major":
                cost = random.uniform(800, 5000)
            else:
                cost = random.uniform(100, 1200)

            # Map failure types to match expected schema
            failure_types = [
                "mechanical_failure",
                "sensor_malfunction",
                "power_failure",
                "software_error",
                "calibration_drift",
                "communication_failure",
            ]
            root_causes = [
                "wear",
                "overheating",
                "vibration",
                "corrosion",
                "fatigue",
                "age",
                "environment",
            ]
            repair_actions = [
                "component_replacement",
                "calibration_adjustment",
                "software_update",
                "cleaning_maintenance",
                "connection_repair",
                "full_overhaul",
            ]

            failure = FailureEvent(
                failure_id=f"FAIL_{device.device_id}_{str(uuid.uuid4())[:8]}",
                device_id=device.device_id,
                failure_timestamp=failure_time.isoformat()
                + ".000000",  # Match expected format
                repair_timestamp=repair_time.isoformat() + ".000000",
                failure_type=random.choice(failure_types),
                severity=severity,
                root_cause=random.choice(root_causes),
                repair_action=random.choice(repair_actions),
                cost=round(cost, 2),
            )

            failure_events.append(asdict(failure))

        return failure_events

    def generate_maintenance_records(self, days: int = 90) -> List[Dict[str, Any]]:
        """Generate maintenance records for specified time period"""
        maintenance_records = []

        # Get maintenance configuration
        maintenance_config = self.config.get("maintenance", {})
        maintenance_probability = maintenance_config.get("maintenance_probability", 0.7)

        # Generate scheduled maintenance for devices with temporal coherence
        for device in self.devices:
            # Most devices should have at least one maintenance in the specified period
            if random.random() < maintenance_probability:
                maintenance_types = [
                    "preventive",
                    "corrective",
                    "predictive",
                    "emergency",
                ]
                maintenance_type = random.choices(
                    maintenance_types, weights=[0.6, 0.2, 0.15, 0.05]
                )[0]

                # Ensure maintenance happens after device installation
                device_install_date = datetime.fromisoformat(
                    device.installation_date.replace(".000000", "")
                )

                # Maintenance should be between installation and now
                max_days_back = min(days, (datetime.now() - device_install_date).days)
                if max_days_back <= 0:
                    continue  # Skip if device too new

                scheduled_date = datetime.now() - timedelta(
                    days=random.randint(1, max_days_back)
                )

                if maintenance_type == "emergency":
                    actual_date = (
                        scheduled_date  # Emergency maintenance happens immediately
                    )
                else:
                    # Small variance between scheduled and actual
                    variance_hours = random.randint(-24, 48)  # -1 day to +2 days
                    actual_date = scheduled_date + timedelta(hours=variance_hours)

                # Duration in minutes
                if maintenance_type == "emergency":
                    duration_minutes = random.randint(60, 480)  # 1-8 hours
                elif maintenance_type == "corrective":
                    duration_minutes = random.randint(120, 360)  # 2-6 hours
                else:
                    duration_minutes = random.randint(60, 240)  # 1-4 hours

                # Parts that might be used
                all_parts = [
                    "sensor_calibration",
                    "filter_replacement",
                    "cable_connector",
                    "battery_backup",
                    "firmware_update",
                    "mounting_bracket",
                    "protective_housing",
                    "communication_module",
                ]

                num_parts = random.randint(0, 3)
                parts_used = ", ".join(
                    random.sample(all_parts, min(num_parts, len(all_parts)))
                )

                # Cost based on duration and parts
                labor_cost = (duration_minutes / 60) * 75.0  # $75/hour
                parts_cost = (
                    len(parts_used.split(", ")) * random.uniform(50, 200)
                    if parts_used
                    else 0
                )
                total_cost = round(labor_cost + parts_cost, 2)

                descriptions = {
                    "preventive": "Routine preventive maintenance check",
                    "corrective": "Corrective maintenance to address identified issues",
                    "predictive": "Predictive maintenance based on sensor data analysis",
                    "emergency": "Emergency repair due to critical failure",
                }

                maintenance = MaintenanceRecord(
                    maintenance_id=f"MAINT_{device.device_id}_{str(uuid.uuid4())[:8]}",
                    device_id=device.device_id,
                    maintenance_type=maintenance_type,
                    scheduled_date=scheduled_date.isoformat() + ".000000",
                    maintenance_date=actual_date.isoformat() + ".000000",
                    duration_minutes=duration_minutes,
                    technician=f"TECH_{random.randint(1001, 1050)}",
                    description=descriptions[maintenance_type],
                    cost=total_cost,
                    parts_replaced=parts_used,
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
        """Generate all types of data with coherence validation"""
        data = {
            "device_master": self.get_device_master_data(),
            "telemetry": self.generate_telemetry_data(telemetry_hours),
            "failures": self.generate_failure_events(failure_days),
            "maintenance": self.generate_maintenance_records(maintenance_days),
        }

        # Validate data coherence
        self._validate_data_coherence(data)

        return data

    def _validate_data_coherence(self, data: Dict[str, List[Dict[str, Any]]]) -> None:
        """Validate that generated data is coherent for joins"""
        device_ids = set([d["device_id"] for d in data["device_master"]])

        # Check telemetry data references valid devices
        telemetry_device_ids = set([t["device_id"] for t in data["telemetry"]])
        invalid_telemetry = telemetry_device_ids - device_ids
        if invalid_telemetry:
            print(
                f"Warning: Telemetry data references {len(invalid_telemetry)} non-existent devices"
            )

        # Check failure data references valid devices
        if data["failures"]:
            failure_device_ids = set([f["device_id"] for f in data["failures"]])
            invalid_failures = failure_device_ids - device_ids
            if invalid_failures:
                print(
                    f"Warning: Failure data references {len(invalid_failures)} non-existent devices"
                )

        # Check maintenance data references valid devices
        if data["maintenance"]:
            maint_device_ids = set([m["device_id"] for m in data["maintenance"]])
            invalid_maint = maint_device_ids - device_ids
            if invalid_maint:
                print(
                    f"Warning: Maintenance data references {len(invalid_maint)} non-existent devices"
                )

        print(f"✅ Data coherence validated:")
        print(f"  - {len(device_ids)} devices in master data")
        print(f"  - {len(telemetry_device_ids)} devices with telemetry data")
        print(f"  - {len(data['failures'])} failure events generated")
        print(f"  - {len(data['maintenance'])} maintenance records generated")

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
                json.dump(records, f)
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

    # Override for demo with more devices and longer time range
    config.num_devices = 100  # Increased from 50

    # Initialize generator with configuration
    generator = IoTDataGenerator(config=config)

    print(f"Generated {len(generator.devices)} devices")

    # Generate all data types with longer time ranges for better ML training
    all_data = generator.generate_all_data(
        telemetry_hours=168,  # 7 days of telemetry data (was 48)
        failure_days=180,  # 6 months of failure events (was 30)
        maintenance_days=365,  # 1 year of maintenance records (was 90)
    )

    # Print summary statistics
    for data_type, records in all_data.items():
        print(f"{data_type.capitalize()}: {len(records)} records")

    # Save to files
    file_paths = generator.save_data_to_json(all_data, config.output_directory)

    print("\nFiles saved:")
    for data_type, path in file_paths.items():
        print(f"  {data_type}: {path}")

    # Print sample device IDs to verify consistency across datasets
    print(f"\n📋 Device ID Consistency Check:")
    device_ids = [d["device_id"] for d in all_data["device_master"][:5]]
    print(f"  Device Master: {device_ids}")

    telemetry_ids = list(set([t["device_id"] for t in all_data["telemetry"][:20]]))[:5]
    print(f"  Telemetry: {telemetry_ids}")

    if all_data["failures"]:
        failure_ids = list(set([f["device_id"] for f in all_data["failures"]]))[:5]
        print(f"  Failures: {failure_ids}")

    if all_data["maintenance"]:
        maint_ids = list(set([m["device_id"] for m in all_data["maintenance"]]))[:5]
        print(f"  Maintenance: {maint_ids}")

    print(f"\n✅ Data generation completed successfully!")
    print(
        f"💡 Run 'python validate_data_coherence.py {config.output_directory}' to validate coherence"
    )


if __name__ == "__main__":
    main()
