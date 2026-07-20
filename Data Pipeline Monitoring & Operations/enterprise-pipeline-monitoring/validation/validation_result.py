from dataclasses import dataclass, field
from datetime import datetime
from typing import List


@dataclass
class ValidationResult:

    validation_name: str

    status: str

    total_records: int

    failed_records: int

    message: str

    execution_time: str = field(
        default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    failed_rows: List[int] = field(default_factory=list)
