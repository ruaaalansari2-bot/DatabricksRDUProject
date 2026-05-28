# DatabricksRDUProject - Setup Guide

## Project Overview
This is a **Medallion-architecture data lakehouse on Databricks** that turns public housing data into market intelligence for the Raleigh-Durham metro.

**Original Repository:** [vsantosh531/DatabricksRDUProject](https://github.com/vsantosh531/DatabricksRDUProject)
**Copied To:** [ruaaalansari2-bot/DatabricksRDUProject](https://github.com/ruaaalansari2-bot/DatabricksRDUProject)

---

## Repository Details

- **Primary Language:** Python
- **Architecture:** Medallion (Bronze → Silver → Gold layers)
- **Tech Stack:** 
  - Databricks
  - Delta Lake
  - PySpark
  - Power BI
  - Real Estate Data
- **Default Branch:** `dev`
- **Branches Available:** `main`, `dev`

---

## Setup Steps Completed ✅

### Step 1: Repository Copy (Completed)
- **Method Used:** Mirror clone and push
- **Date:** May 28, 2026
- **Status:** ✅ Successfully copied all 186 objects

**Commands Executed:**
```bash
# Clone as mirror from original repository
git clone --mirror https://github.com/vsantosh531/DatabricksRDUProject.git

# Navigate into the cloned repository
cd DatabricksRDUProject.git

# Push mirror to new repository (with authentication token)
git push --mirror https://github.com/ruaaalansari2-bot/DatabricksRDUProject.git

# Clean up
cd ..
rmdir /s /q DatabricksRDUProject.git
```

### Step 2: Local Repository Setup (Completed)
- **Location:** `C:\Users\AnsarRua\Projects\DatabricksRDUProject`
- **Status:** ✅ Successfully cloned locally

**Commands Executed:**
```bash
# Create Projects directory
mkdir C:\Users\AnsarRua\Projects
cd C:\Users\AnsarRua\Projects

# Clone the repository
git clone https://github.com/ruaaalansari2-bot/DatabricksRDUProject.git

# Navigate into project
cd DatabricksRDUProject
```

### Step 3: Verification (Completed)
All branches are available locally:
- `main` (default)
- `dev` (development)
- Remote tracking branches set up correctly

---

## Next Steps ⏳

See **TODO.md** for the upcoming tasks to test this project on Databricks.

---

## Prerequisites for Testing

Before running this project, you'll need:

1. **Databricks Account** (Free tier available)
   - [Create Free Account](https://databricks.com/try-databricks)
   - Options: Community Edition (free forever) or 14-day free trial

2. **Python Environment** (Local)
   - Python 3.8+
   - PySpark libraries
   - Databricks SDK

3. **Data Files**
   - Public housing data for Raleigh-Durham area
   - CSV or Parquet format

4. **Cloud Provider Account** (optional for free tier)
   - AWS, Azure, or GCP account (may be needed for enterprise features)

---

## Project Structure

```
DatabricksRDUProject/
├── notebooks/          # Databricks notebooks (Python, SQL)
├── data/              # Raw and processed data files
├── src/               # Python source code
├── config/            # Configuration files
├── README.md          # Project documentation
└── SETUP_GUIDE.md     # This file
```

---

## Useful Resources

- [Databricks Documentation](https://docs.databricks.com/)
- [Delta Lake Guide](https://docs.delta.io/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

---

## Quick Reference: Key Branches

| Branch | Purpose | Status |
|--------|---------|--------|
| `main` | Production-ready code | Latest stable |
| `dev` | Development & testing | Active development |

---

## Notes

- All repository content has been successfully mirrored
- Both `main` and `dev` branches contain all commits and history
- Ready for Databricks workspace setup and testing
- See TODO.md for next actions

---

**Last Updated:** May 28, 2026
**Setup By:** ruaaalansari2-bot
