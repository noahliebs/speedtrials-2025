# SDWIS Georgia Q1 2025 ETL Loader

This project contains a cleaned ETL pipeline to ingest Georgia’s Q1 2025 Safe Drinking Water Information System (SDWIS) data into a PostgreSQL database.

---

## 🚀 How to Run

> Make sure PostgreSQL is running and you have a database created.

1. Create the database if needed:

```bash
psql -c "CREATE DATABASE sdwis_georgia;"
```

2. Run the loader:

```bash
cd etl
chmod +x data_load.sh
./data_load.sh
```

---

## 🧼 Data Cleaning Highlights

The ETL process includes smart data validation and cleanup:

### ✅ Bug 1: Missing `violation_id`
- Rows missing this required field are skipped.
- Logged to `violations_skipped.log`.

### ✅ Bug 2: Filler characters like `--->`
- Replaced with NULLs during cleanup.

### ✅ Bug 3: Extra trailing commas
- Some rows had 39 fields when 38 expected.
- Cleaner trims blank columns at the end.

### ✅ Bug 4: Improperly quoted NULLs (`""`)
- Converted to proper nulls so date parsing and numeric conversion work in PostgreSQL.

---

## 📊 Summary Output

After loading, the script prints:

- ✅ Row counts per table
- ✅ System type distribution
- ✅ Violation status breakdown
- ✅ Health-based vs non-health-based counts
- ✅ Average violations per water system

---
