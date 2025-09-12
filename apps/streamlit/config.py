from typing import List, Set

# Configure default warehouse and allowlists
DEFAULT_WAREHOUSE = "PIPELINE_WH"

# Optionally restrict which databases/schemas/tables can be queried
ALLOWED_DATABASES: Set[str] = set()
ALLOWED_SCHEMAS: Set[str] = set()
ALLOWED_TABLES: Set[str] = set()  # e.g., {"RAW.SALES.ORDERS", "RAW.CRM.CUSTOMERS"}

# Maximum preview rows
PREVIEW_LIMIT = 50

# Cortex model name (subject to account availability)
CORTEX_MODEL = "mistral-large"

# Few-shot examples to steer SQL generation (optional)
FEW_SHOTS: List[str] = [
    "You are a SQL assistant for Snowflake. Only output a single SELECT statement with fully qualified identifiers. No DML/DDL, no comments.",
]
