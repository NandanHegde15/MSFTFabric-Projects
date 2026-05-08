# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# PARAMETERS CELL ********************

# ──────────────────────────────────────────────────────────────────────────────────
#  CONFIGURATION — update these values / Pipeline params
# ──────────────────────────────────────────────────────────────────────────────────
WORKSPACE_ID = ""
LAKEHOUSE_ID = ""
JSON_FILE_PATH = "Files/raw/fhir/coverage/08042026122125_Coverage.json"
TARGET_TABLE = "Coverage"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ABFSS_PATH = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
    f"/{LAKEHOUSE_ID}/Files/{JSON_FILE_PATH}"
)
ABFSS_TABLE_PATH = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
    f"/{LAKEHOUSE_ID}/Tables/{TARGET_TABLE}"
)


print(f" Source path : {ABFSS_PATH}")
print(f" Target table: {TARGET_TABLE}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType, StringType
from pyspark.sql.functions import col, count, when
from notebookutils import mssparkutils

file_info = mssparkutils.fs.ls(ABFSS_PATH)[0]
size_in_bytes = file_info.size

# ── Read JSON (multiline = one JSON object may span multiple lines) ──
df_raw = (
    spark.read
         .json(ABFSS_PATH)
).select("entry")

print(f" Loaded {df_raw.count()} rows from: {JSON_FILE_PATH}")
# display(df_raw.limit(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# print("" * 60)
# print(" RAW SCHEMA")
# print("" * 60)
# df_raw.printSchema()

# print(f" Row count  : {df_raw.count()}")
# print(f" Column count: {len(df_raw.columns)}")
# print(f" Top-level columns: {df_raw.columns}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ─────────────────────────────────────────────────────────
# Helper: check whether the DataFrame still has nested types
# ─────────────────────────────────────────────────────────
def _has_nested(df):
    """Return True if any column is still a StructType or ArrayType."""
    for field in df.schema.fields:
        if isinstance(field.dataType, (StructType, ArrayType)):
            return True
    return False


# ─────────────────────────────────────────────────────────
# Core: one pass of flattening
# ─────────────────────────────────────────────────────────
def _flatten_one_pass(df):
    """
    One flattening pass:
      • ArrayType  → explode (inline for arrays-of-structs, else regular explode)
      • StructType → expand every sub-field as a top-level column using dot notation
    All other columns are kept as-is.
    """
    columns = []
    for field in df.schema.fields:
        col_name = field.name
        data_type = field.dataType

        if isinstance(data_type, ArrayType):
            # Array-of-structs → explode_outer keeps NULLs; use inline for struct arrays
            if isinstance(data_type.elementType, StructType):
                df = df.withColumn(col_name, F.explode_outer(F.col(f"`{col_name}`")))
                # After explode the column is now a StructType → expand in next pass
                columns.append(F.col(f"`{col_name}`"))
            else:
                # Primitive array → explode to scalar values
                df = df.withColumn(col_name, F.explode_outer(F.col(f"`{col_name}`")))
                columns.append(F.col(f"`{col_name}`"))

        elif isinstance(data_type, StructType):
            # Expand each struct field as a new top-level column
            for sub_field in data_type.fields:
                new_col_name = f"{col_name}.{sub_field.name}"
                columns.append(
                    F.col(f"`{col_name}`.`{sub_field.name}`").alias(new_col_name)
                )
        else:
            columns.append(F.col(f"`{col_name}`"))

    return df.select(columns)


# ─────────────────────────────────────────────────────────
# Public: fully recursive flattener
# ─────────────────────────────────────────────────────────
def flatten_json_df(df, max_iterations=30):
    """
    Repeatedly flatten the DataFrame until no StructType or ArrayType
    columns remain (i.e. every column is a primitive scalar).

    Parameters
    ----------
    df             : PySpark DataFrame
    max_iterations : safety cap to prevent infinite loops
    """
    iteration = 0
    while _has_nested(df) and iteration < max_iterations:
        iteration += 1
        print(f"  ↳ Pass {iteration}: {len(df.columns)} columns → flattening ...")
        df = _flatten_one_pass(df)

    print(f"\n Done after {iteration} pass(es). Final column count: {len(df.columns)}")
    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re

# ── Step 1: Fully flatten ────────────────────────────────
print(" Starting recursive flattening ...")
df_flat = flatten_json_df(df_raw)

# ── Step 2: Sanitise column names ───────────────────────
# Replace dots, spaces, dashes, brackets and other special chars with _
def sanitise_col(name: str) -> str:
    """Replace any non-alphanumeric character (except _) with underscore,
    collapse consecutive underscores, strip leading/trailing underscores."""
    name = re.sub(r"[^0-9a-zA-Z_]", "_", name)   # replace special chars
    name = re.sub(r"_+", "_", name)               # collapse consecutive _
    name = name.strip("_")                        # strip leading/trailing _
    return name.lower()                           # lowercase for consistency

rename_map = {c: sanitise_col(c) for c in df_flat.columns}

# Detect and resolve duplicate names after sanitisation
seen = {}
for old, new in rename_map.items():
    if new in seen.values():
        suffix = sum(1 for v in seen.values() if v == new or v.startswith(new + "_"))
        rename_map[old] = f"{new}_{suffix}"
    seen[old] = rename_map[old]

for old_name, new_name in rename_map.items():
    if old_name != new_name:
        df_flat = df_flat.withColumnRenamed(old_name, new_name)

# print("\n Flattened schema:")
# df_flat.printSchema()

print(f"Flattened rows   : {df_flat.count()}")
print(f"Flattened columns: {len(df_flat.columns)}")
# display(df_flat.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Count non-null values for each column
non_null_counts = df_flat.select([
    count(when(col(c).isNotNull(), c)).alias(c)
    for c in df_flat.columns
]).collect()[0].asDict()

# Keep only columns with at least 1 non-null value
cols_to_keep = [c for c, v in non_null_counts.items() if v > 0]

df_clean = df_flat.select(*cols_to_keep)

# display(df_clean.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Writing to managed Delta table: [{TARGET_TABLE}] ...")

(
    df_clean.write
           .format("delta")
           .mode("overwrite")                        # overwrite on re-runs
           .option("overwriteSchema", "true")        # allow schema evolution
           .save(ABFSS_TABLE_PATH)
)

print(f"Table [{TARGET_TABLE}] created/overwritten successfully!")


#notebookutils.notebook.exit([df_filtered.count(), size_in_bytes])
notebookutils.notebook.exit([
    "RowCount: " + str(df_clean.count()),
    "FileSize: " + str(size_in_bytes)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
