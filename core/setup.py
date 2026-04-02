#!/usr/bin/env python
import glob
import os
import sys

if sys.version_info < (3, 9):
    print("Error: dbt does not support this version of Python.")
    print("Please upgrade to Python 3.9 or higher.")
    sys.exit(1)


from setuptools import setup, Extension

try:
    from setuptools import find_namespace_packages
except ImportError:
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print(
        'Please upgrade setuptools with "pip install --upgrade setuptools" '
        "and try again"
    )
    sys.exit(1)


this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()


package_name = "dvt-ce"
package_version = "0.1.43"
description = """DVT — cross-engine data transformation tool with DuckDB federation."""


# ---------------------------------------------------------------------------
# Cython compilation — compile .py to .so for source code protection
# ---------------------------------------------------------------------------

# Files that MUST stay as .py (dynamic imports, namespace packages)
EXCLUDE_FROM_CYTHON = {
    # Namespace packages with extend_path / pkgutil
    os.path.join("dbt", "__init__.py"),
    os.path.join("dbt", "include", "__init__.py"),
    # Dynamic plugin/adapter discovery (importlib.import_module, __import__)
    os.path.join("dbt", "version.py"),
    os.path.join("dbt", "plugins", "manager.py"),
    # Resilient CLI entry point (catches ImportError for fallback)
    os.path.join("dvt", "cli", "__init__.py"),
    # Dynamic adapter/driver checking (__import__ for adapter names)
    os.path.join("dvt", "tasks", "sync.py"),
    os.path.join("dvt", "sync", "adapter_installer.py"),
    os.path.join("dvt", "sync", "cloud_deps.py"),
}

ext_modules = []

try:
    from Cython.Build import cythonize

    # Collect all .py files under dbt/ and dvt/
    all_py = glob.glob("dbt/**/*.py", recursive=True) + glob.glob(
        "dvt/**/*.py", recursive=True
    )

    to_compile = []
    for f in all_py:
        # Skip excluded files
        if f in EXCLUDE_FROM_CYTHON:
            continue
        # Skip all __init__.py (namespace package markers)
        if os.path.basename(f) == "__init__.py":
            continue
        # Skip non-Python assets in dbt/include/
        if f.startswith(os.path.join("dbt", "include", "starter_project")):
            continue
        # Skip test fixtures and docs
        if f.startswith(os.path.join("dbt", "tests")):
            continue
        if f.startswith(os.path.join("dbt", "docs")):
            continue
        # Skip any top-level files outside dbt/dvt packages
        if not (f.startswith("dbt" + os.sep) or f.startswith("dvt" + os.sep)):
            continue
        to_compile.append(f)

    ext_modules = cythonize(
        to_compile,
        compiler_directives={"language_level": "3"},
        quiet=True,
    )
    print(f"Cython: compiling {len(to_compile)} modules")

except ImportError:
    # Cython not installed — build as pure Python (development mode)
    print("Cython not found — building as pure Python")


setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Hesham Badawi",
    author_email="hesham.h.96@gmail.com",
    url="https://github.com/heshamh96/dvt-ce",
    packages=find_namespace_packages(include=["dbt", "dbt.*", "dvt", "dvt.*"]),
    include_package_data=True,
    test_suite="test",
    entry_points={
        "console_scripts": [
            "dvt = dvt.cli:cli",
            "dbt = dbt.cli.main:cli",
        ],
    },
    ext_modules=ext_modules,
    install_requires=[
        # ----
        # dbt-core uses these packages deeply, throughout the codebase, and there have been breaking changes in past patch releases (even though these are major-version-one).
        # Pin to the patch or minor version, and bump in each new minor version of dbt-core.
        "agate>=1.7.0,<1.10",
        "Jinja2>=3.1.3,<4",
        "mashumaro[msgpack]>=3.9,<3.15",
        # ----
        # dbt-core uses these packages in standard ways. Pin to the major version, and check compatibility
        # with major versions in each new minor version of dbt-core.
        "click>=8.0.2,<9.0",
        "networkx>=2.3,<4.0",
        "protobuf>=6.0,<7.0",
        "requests<3.0.0",  # should match dbt-common
        "posthog>=3.0.0,<4.0",
        # ----
        # These packages are major-version-0. Keep upper bounds on upcoming minor versions (which could have breaking changes)
        # and check compatibility / bump in each new minor version of dbt-core.
        "pathspec>=0.9,<0.13",
        "sqlparse>=0.5.0,<0.6.0",
        # ----
        # These are major-version-0 packages also maintained by dbt-labs.
        # Accept patches but avoid automatically updating past a set minor version range.
        "dbt-extractor>=0.5.0,<=0.6",
        "dbt-semantic-interfaces>=0.7.4,<0.8",
        # Minor versions for these are expected to be backwards-compatible
        "dbt-common>=1.13.0,<2.0",
        "dvt-adapters>=0.1.23",
        # ----
        # Expect compatibility with all new versions of these packages, so lower bounds only.
        "packaging>20.9",
        "pytz>=2015.7",
        "pyyaml>=6.0",
        "daff>=1.3.46",
        "typing-extensions>=4.4",
        # ----
        # DVT-specific: Sling data movement + DuckDB cross-engine compute
        "sling>=1.2.0",
        "sqlglot>=20.0.0",
        "duckdb>=0.9.0",
        "pyarrow>=14.0.0",
        # ----
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.9",
)
