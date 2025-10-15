#!/usr/bin/env python3
"""
CLI script for running the IoT Data Generator
"""

import argparse
import os
import sys
from pathlib import Path

# Add src to path so we can import our modules
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from data_simulation.iot_data_generator import IoTDataGenerator
from data_simulation.azure_uploader import upload_iot_data_to_azure


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic IoT data")

    # Data generation parameters
    parser.add_argument(
        "--devices",
        type=int,
        default=100,
        help="Number of devices to simulate (default: 100)",
    )
    parser.add_argument(
        "--telemetry-hours",
        type=int,
        default=24,
        help="Hours of telemetry data to generate (default: 24)",
    )
    parser.add_argument(
        "--failure-days",
        type=int,
        default=30,
        help="Days of failure events to generate (default: 30)",
    )
    parser.add_argument(
        "--maintenance-days",
        type=int,
        default=90,
        help="Days of maintenance records to generate (default: 90)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Telemetry interval in minutes (default: 5)",
    )

    # Output options
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./output",
        help="Output directory for generated files (default: ./output)",
    )
    parser.add_argument(
        "--format",
        choices=["json"],
        default="json",
        help="Output format (default: json)",
    )

    # Azure options
    parser.add_argument(
        "--upload-to-azure",
        action="store_true",
        help="Upload generated data to Azure Data Lake",
    )
    parser.add_argument(
        "--storage-account", type=str, help="Azure storage account name"
    )
    parser.add_argument("--storage-key", type=str, help="Azure storage account key")
    parser.add_argument(
        "--container",
        type=str,
        default="raw",
        help="Azure container name (default: raw)",
    )

    # Other options
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run with demo settings (fewer devices, shorter time periods)",
    )

    args = parser.parse_args()

    # Demo mode overrides
    if args.demo:
        args.devices = 20
        args.telemetry_hours = 6
        args.failure_days = 7
        args.maintenance_days = 30
        print("Demo mode: Using reduced data volumes")

    # Verbose output
    if args.verbose:
        print(f"Configuration:")
        print(f"  Devices: {args.devices}")
        print(f"  Telemetry: {args.telemetry_hours} hours")
        print(f"  Failures: {args.failure_days} days")
        print(f"  Maintenance: {args.maintenance_days} days")
        print(f"  Output: {args.output_dir}")
        if args.upload_to_azure:
            print(f"  Azure storage: {args.storage_account}")
            print(f"  Azure container: {args.container}")
        print()

    # Initialize data generator
    print(f"Initializing IoT Data Generator with {args.devices} devices...")
    generator = IoTDataGenerator()
    generator.num_devices = args.devices

    # Generate data
    print("Generating synthetic IoT data...")
    all_data = generator.generate_all_data(
        telemetry_hours=args.telemetry_hours,
        failure_days=args.failure_days,
        maintenance_days=args.maintenance_days,
    )

    # Print summary
    print("\nData generation complete:")
    for data_type, records in all_data.items():
        print(f"  {data_type.capitalize()}: {len(records)} records")

    # Save to local files
    print(f"\nSaving data to {args.output_dir}...")
    file_paths = generator.save_data_to_json(all_data, args.output_dir)

    # Upload to Azure if requested
    if args.upload_to_azure:
        if not args.storage_account:
            # Try to get from environment
            args.storage_account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
            if not args.storage_account:
                print("Error: --storage-account required for Azure upload")
                sys.exit(1)

        if not args.storage_key:
            args.storage_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

        print(f"\nUploading data to Azure Data Lake...")
        print(f"  Storage Account: {args.storage_account}")
        print(f"  Container: {args.container}")

        try:
            uploaded_paths = upload_iot_data_to_azure(
                iot_data=all_data,
                storage_account_name=args.storage_account,
                container_name=args.container,
                storage_account_key=args.storage_key,
            )

            print("\nAzure upload complete:")
            for data_type, path in uploaded_paths.items():
                print(f"  {data_type}: {path}")

        except Exception as e:
            print(f"Error uploading to Azure: {str(e)}")
            if args.verbose:
                import traceback

                traceback.print_exc()
            sys.exit(1)

    print("\nIoT data generation completed successfully!")


if __name__ == "__main__":
    main()
