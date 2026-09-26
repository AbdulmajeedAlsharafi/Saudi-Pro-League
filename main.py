"""Main entry point for the Saudi Pro League ETL pipeline.

The existing notebook logic has been refactored into reusable stage modules
under src/ while preserving the Bronze -> Silver -> Validation -> Gold flow.
"""

from src.extract import run_extraction
from src.clean import run_profile_clean
from src.schema import run_schema_validation
from src.transform import run_join_transform


def main():
    print("=== Saudi Pro League ETL Pipeline ===")

    print("\n[1/4] Extract -> Bronze")
    run_extraction()

    print("\n[2/4] Profile & Clean -> Silver")
    run_profile_clean()

    print("\n[3/4] Schema Validation")
    run_schema_validation()

    print("\n[4/4] Join & Transform -> Gold")
    run_join_transform()

    print("\n=== Pipeline completed ===")


if __name__ == "__main__":
    main()
