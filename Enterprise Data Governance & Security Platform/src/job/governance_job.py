# ============================================================
# File: governance_job.py
# Purpose: Production job entry point
# ============================================================

from jobs.governance_pipeline import run_pipeline


def main():

    print(
        "Starting Enterprise "
        "Data Governance Job"
    )

    result = run_pipeline()

    print(
        f"Governance job completed: "
        f"{result}"
    )

    if result["status"] != "SUCCESS":

        raise RuntimeError(
            "Governance pipeline failed. "
            "Review audit table."
        )


if __name__ == "__main__":

    main()
