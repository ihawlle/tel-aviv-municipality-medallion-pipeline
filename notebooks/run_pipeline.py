# Databricks notebook source
# MAGIC %md
# MAGIC # Tel Aviv Medallion Pipeline Runner

# COMMAND ----------

# If Snowflake export fails due to missing Python deps, add at the top of the notebook:
# %pip install snowflake-connector-python
#
# If you see "404 Not Found" on Snowflake login, set SNOWFLAKE_ACCOUNT to your correct
# account identifier (from Snowsight: View account details -> Account/Server URL, without .snowflakecomputing.com):
# os.environ["SNOWFLAKE_ACCOUNT"] = "your-org-your-account"  # or locator.region.cloud

# Setup for Databricks bundle deployment: find project root from cwd (avoids .bundle paths
# that cause __pycache__ FileNotFoundError) and suppress bytecode generation.
import os
import sys
import importlib
from pathlib import Path


def _find_project_root_fixed() -> str:
    """
    Locate project root containing src/pipeline/main.py by walking up from cwd.
    Avoids .bundle paths and works reliably from Databricks job cwd.
    """
    candidate = Path(os.getcwd())
    for _ in range(6):
        main_py = candidate / "src" / "pipeline" / "main.py"
        if main_py.exists():
            return str(candidate)
        candidate = candidate.parent
    raise RuntimeError(
        f"Could not locate project root containing src/pipeline/main.py. Current path: {os.getcwd()}"
    )


project_root = _find_project_root_fixed()
if project_root not in sys.path:
    sys.path.insert(0, project_root)
print("Project root:", project_root)

# Prevent Python from creating __pycache__/.pyc in workspace (avoids FileNotFoundError).
sys.dont_write_bytecode = True

# Force reload in case Databricks notebook session cached an older module version.
import src.pipeline.bronze as bronze_module
import src.pipeline.silver as silver_module
import src.pipeline.gold as gold_module
import src.pipeline.main as main_module

importlib.reload(bronze_module)
importlib.reload(silver_module)
importlib.reload(gold_module)
importlib.reload(main_module)

print("Loaded bronze from:", bronze_module.__file__)
print("Loaded main from:", main_module.__file__)

outputs = main_module.run_pipeline(spark)

for layer_name, layer_outputs in outputs.items():
    for output_name, df in layer_outputs.items():
        print(f"{layer_name}.{output_name}: {df.count()} rows")

# COMMAND ----------

# Display primary business output.
outputs["gold"]["business_annual_gold"].orderBy("annual_compensation_ils", ascending=False).display()

# COMMAND ----------

# Audit sample (assignment audit with detailed failure reasons).
outputs["gold"]["gold_audit_assignment_unmatched"].orderBy("date", ascending=False).display()
