#!/usr/bin/env python3
"""
Phase 3 File Organizer for WWI Fabric Migration Project

This script organizes the downloaded Phase 3 Silver Layer files 
into the proper Git repository structure.

Usage:
    python organize_phase3_files.py <source_folder> <repo_folder>

Example:
    python organize_phase3_files.py ./wwi-phase3-silver-layer ./WWI-Fabric-DataPlatform
"""

import os
import shutil
import argparse
from pathlib import Path


def create_directory(path: Path) -> None:
    """Create directory if it doesn't exist."""
    if not path.exists():
        path.mkdir(parents=True)
        print(f"  📁 Created: {path}")


def copy_file(src: Path, dest: Path) -> None:
    """Copy file and print status."""
    if src.exists():
        shutil.copy2(src, dest)
        print(f"  ✅ Copied: {src.name} → {dest}")
    else:
        print(f"  ❌ Not found: {src}")


def organize_phase3_files(source_folder: str, repo_folder: str) -> None:
    """
    Organize Phase 3 files into the Git repository structure.
    
    Target structure:
    WWI-Fabric-DataPlatform/
    ├── docs/
    │   ├── architecture/
    │   │   └── phase3_silver_layer_plan.md
    │   └── runbooks/
    │       └── phase3_implementation_guide.md
    ├── src/
    │   ├── fabric/
    │   │   ├── notebooks/
    │   │   │   └── silver/
    │   │   │       └── (all notebook files)
    │   │   └── pipelines/
    │   │       └── silver/
    │   │           └── pl_silver_master_load.json
    │   └── sql/
    │       └── silver/
    │           └── silver_control_tables_setup.md
    """
    
    source = Path(source_folder)
    repo = Path(repo_folder)
    
    # Validate paths
    if not source.exists():
        print(f"❌ Error: Source folder not found: {source}")
        return
    
    if not repo.exists():
        print(f"❌ Error: Repository folder not found: {repo}")
        return
    
    print("=" * 60)
    print("Phase 3 File Organizer")
    print("=" * 60)
    print(f"Source: {source.absolute()}")
    print(f"Target: {repo.absolute()}")
    print("=" * 60)
    
    # Define target directories
    dirs = {
        "docs_arch": repo / "docs" / "architecture",
        "docs_runbooks": repo / "docs" / "runbooks",
        "notebooks_silver": repo / "src" / "fabric" / "notebooks" / "silver",
        "pipelines_silver": repo / "src" / "fabric" / "pipelines" / "silver",
        "sql_silver": repo / "src" / "sql" / "silver",
    }
    
    # Create directories
    print("\n📁 Creating directories...")
    for name, path in dirs.items():
        create_directory(path)
    
    # Define file mappings (source -> destination)
    file_mappings = [
        # Documentation
        (
            source / "00_phase3_master_plan.md",
            dirs["docs_arch"] / "phase3_silver_layer_plan.md"
        ),
        (
            source / "02_implementation_guide.md",
            dirs["docs_runbooks"] / "phase3_implementation_guide.md"
        ),
        
        # SQL/Control tables
        (
            source / "01_silver_control_tables.md",
            dirs["sql_silver"] / "silver_control_tables_setup.md"
        ),
        
        # Pipeline
        (
            source / "pipelines" / "pl_silver_master_load.json",
            dirs["pipelines_silver"] / "pl_silver_master_load.json"
        ),
    ]
    
    # Notebooks (all .py files in notebooks folder)
    notebooks_source = source / "notebooks"
    if notebooks_source.exists():
        for notebook_file in notebooks_source.glob("*.py"):
            file_mappings.append((
                notebook_file,
                dirs["notebooks_silver"] / notebook_file.name
            ))
    
    # Copy files
    print("\n📄 Copying files...")
    copied_count = 0
    for src, dest in file_mappings:
        if src.exists():
            copy_file(src, dest)
            copied_count += 1
        else:
            print(f"  ⚠️  Skipped (not found): {src}")
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"✅ Copied {copied_count} files")
    print(f"\nFiles are now organized in: {repo.absolute()}")
    
    # Print tree structure of what was created
    print("\n📂 Repository structure (Phase 3 files):")
    print(f"""
{repo.name}/
├── docs/
│   ├── architecture/
│   │   └── phase3_silver_layer_plan.md
│   └── runbooks/
│       └── phase3_implementation_guide.md
├── src/
│   ├── fabric/
│   │   ├── notebooks/
│   │   │   └── silver/
│   │   │       ├── nb_silver_utilities.py
│   │   │       ├── nb_silver_reference_tables.py
│   │   │       ├── nb_silver_master_customer.py
│   │   │       ├── nb_silver_master_supplier.py
│   │   │       ├── nb_silver_master_stock_item.py
│   │   │       ├── nb_silver_master_person.py
│   │   │       ├── nb_silver_txn_orders.py
│   │   │       ├── nb_silver_txn_invoices.py
│   │   │       ├── nb_silver_txn_purchases.py
│   │   │       ├── nb_silver_txn_financials.py
│   │   │       ├── nb_silver_inventory.py
│   │   │       └── nb_silver_iot_temperatures.py
│   │   └── pipelines/
│   │       └── silver/
│   │           └── pl_silver_master_load.json
│   └── sql/
│       └── silver/
│           └── silver_control_tables_setup.md
""")
    
    print("\n💡 Next steps:")
    print("   1. cd into your repo folder")
    print("   2. git add .")
    print("   3. git commit -m 'Add Phase 3 Silver Layer implementation'")
    print("   4. git push")


def main():
    parser = argparse.ArgumentParser(
        description="Organize Phase 3 Silver Layer files into Git repository structure"
    )
    parser.add_argument(
        "source_folder",
        help="Path to the downloaded wwi-phase3-silver-layer folder"
    )
    parser.add_argument(
        "repo_folder", 
        help="Path to your WWI-Fabric-DataPlatform Git repository"
    )
    
    args = parser.parse_args()
    organize_phase3_files(args.source_folder, args.repo_folder)


if __name__ == "__main__":
    main()
