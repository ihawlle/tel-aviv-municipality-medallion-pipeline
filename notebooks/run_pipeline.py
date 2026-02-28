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
import importlib.util
import types
from pathlib import Path


def _find_project_root_fixed() -> str:
    """
    Locate project root containing src/pipeline/main.py.
    Handles Databricks bundle deployment and Repos layouts.
    """
    # If __file__ is set (e.g. notebook run as script), use it
    try:
        notebook_path = Path(__file__).resolve()
        if notebook_path.name == "run_pipeline.py":
            candidate = notebook_path.parent.parent  # notebooks/ -> project root
            if (candidate / "src" / "pipeline" / "main.py").exists():
                return str(candidate)
    except NameError:
        pass

    candidate = Path(os.getcwd())
    for _ in range(8):  # Deeper walk for bundle/Repos paths
        if (candidate / "src" / "pipeline" / "main.py").exists():
            return str(candidate)
        candidate = candidate.parent

    raise RuntimeError(
        f"Could not locate project root. cwd={os.getcwd()}"
    )


def _ensure_src_packages(project_root: str) -> None:
    """Ensure src and src.pipeline are proper packages so relative imports work."""
    root = Path(project_root)
    src_path = root / "src"
    pipeline_path = src_path / "pipeline"
    if "src" not in sys.modules:
        m = types.ModuleType("src")
        m.__path__ = [str(src_path)]
        m.__package__ = "src"
        sys.modules["src"] = m
    if "src.pipeline" not in sys.modules:
        m = types.ModuleType("src.pipeline")
        m.__path__ = [str(pipeline_path)]
        m.__package__ = "src.pipeline"
        sys.modules["src.pipeline"] = m


def _load_pipeline_module(project_root: str, name: str):
    """Load a pipeline module by path; bypasses package resolution issues in Databricks."""
    root = Path(project_root)
    file_path = root / "src" / "pipeline" / f"{name}.py"
    if not file_path.exists():
        raise FileNotFoundError(f"Pipeline module not found: {file_path}")
    _ensure_src_packages(project_root)
    mod_name = f"src.pipeline.{name}"
    spec = importlib.util.spec_from_file_location(mod_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load spec for {mod_name}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return mod


project_root = _find_project_root_fixed()
if project_root not in sys.path:
    sys.path.insert(0, project_root)
print("Project root:", project_root)

# Prevent Python from creating __pycache__/.pyc in workspace (avoids FileNotFoundError).
sys.dont_write_bytecode = True

# Load pipeline modules by path (robust for Databricks bundle/Repos layouts).
bronze_module = _load_pipeline_module(project_root, "bronze")
silver_module = _load_pipeline_module(project_root, "silver")
gold_module = _load_pipeline_module(project_root, "gold")
main_module = _load_pipeline_module(project_root, "main")

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
