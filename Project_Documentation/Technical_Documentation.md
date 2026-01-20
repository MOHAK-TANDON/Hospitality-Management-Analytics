# Technical Documentation - Hospitality Data Engineering Project

**Shri Radhe Govind Ji! 🙏**

## Table of Contents
1. [System Architecture](#system-architecture)
2. [Data Pipeline Details](#data-pipeline-details)
3. [Implementation Guide](#implementation-guide)
4. [Data Transformations](#data-transformations)
5. [KPI Formulas](#kpi-formulas)
6. [Performance Optimization](#performance-optimization)
7. [Troubleshooting](#troubleshooting)

---

## System Architecture

### Technology Stack

| Component          | Technology                      | Purpose                          |
|--------------------|---------------------------------|----------------------------------|
| **Platform**       | Databricks                      | Unified analytics platform       |
| **Storage**        | Delta Lake                      | ACID transactions, time travel   |
| **Catalog**        | Unity Catalog                   | Data governance and security     |
| **Compute**        | Apache Spark (PySpark)          | Distributed data processing      |
| **Language**       | Python 3.9+                     | ETL logic implementation         |
| **Data Format**    | JSON (Bronze), Delta (Silver/Gold) | Source and target formats     |

### Catalog Structure

```
hospitality_project (CATALOG)
├── bronze_schema (SCHEMA)
│   └── raw (VOLUME - /Volumes/hospitality_project/bronze_schema/raw/)
│       ├── guests/             (~5,150 records)
│       ├── hotel_inventory/    (~2,000 records)
│       ├── reservations/       (~8,150 records)
│       ├── pos_transactions/   (~15,200 records)
│       ├── housekeeping_logs/  (~12,150 records)
│       ├── _checkpoints/       (Auto Loader state)
│       └── _schemas/           (Inferred schemas)
│
├── silver_schema (SCHEMA)
│   ├── dim_guests              (Delta Table - SCD Type 2)
│   ├── fact_stays_unified      (Delta Table - Partitioned by check_in_date)
│   └── fact_room_availability_daily (Delta Table - Partitioned by date)
│
└── gold_schema (SCHEMA)
    ├── kpi_revpar              (Delta Table - Partitioned by date)
    ├── kpi_adr                 (Delta Table - Partitioned by date)
    ├── kpi_ancillary_attachment_rate (Delta Table - Partitioned by date)
    ├── kpi_housekeeping_turnover_time (Delta Table - Partitioned by date)
    ├── kpi_weekend_vs_weekday_revenue (Delta Table - Partitioned by date)
    └── kpi_executive_dashboard (Delta Table)
```

---

## Data Pipeline Details

### Bronze Layer - Data Generation

#### File: `05_hospitality_data_setup.py`

**Purpose**: Generate realistic hospitality data with intentional data quality issues for training.

**Key Features**:
- Uses **Faker** library for realistic data generation
- Creates ~43,000+ records across 5 tables
- Implements business logic:
  - **Dynamic Pricing**: Weekend prices are 2x weekday prices
  - **Overbooking**: Intentional double-booking scenarios
  - **Late Checkout**: POS transactions after checkout time
  - **Loyalty Tier Evolution**: Guests upgrade over time (for SCD Type 2)

**Intentional Data Quality Issues**:

```python
# Examples from the code:
1. Duplicates (guest_id variations, email+alt@domain)
2. Missing values (NULL loyalty_tier, email, dates)
3. Format inconsistencies (dd/MM/yyyy vs yyyy-MM-dd)
4. Invalid values (negative prices, "invalid-email")
5. Orphaned records (res_id referencing non-existent data)
6. Mixed case (BRONZE vs bronze vs Bronze)
7. Extreme outliers (prices > $10,000)
```

**Generated Relationships**:
```
reservations → guests (via guest_id) - 95% valid, 5% orphaned
reservations → hotel_inventory (via hotel_id, room_number)
pos_transactions → reservations (via res_id) - 85% valid, 15% walk-ins
housekeeping_logs → hotel_inventory (via hotel_id, room_number)
```

---

### Silver Layer - Data Transformation

Two implementations are provided (both produce identical results):

#### Option 1: `bronze_to_silver_auto_loader.py` (RECOMMENDED)

**Key Features**:
- ✅ Auto Loader with Cloud Files
- ✅ Streaming APIs (`readStream`, `writeStream`)
- ✅ Trigger-Once for batch-style processing
- ✅ Schema evolution enabled
- ✅ Rescued data column for malformed records
- ✅ TRUE SCD Type 2 with MERGE operations
- ✅ Checkpoint management

**Auto Loader Configuration**:
```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load(source_path))
```

**SCD Type 2 Implementation (MERGE-based)**:

Step 1: Detect tier changes
```python
# Join incoming data with current dimension
tier_changes = (
    incoming_guests
    .join(current_dim, on='guest_id', how='left')
    .filter(col('loyalty_tier') != col('curr_tier'))
)
```

Step 2: Expire old records via MERGE
```python
dim_guests_delta.alias('target').merge(
    changed_guest_ids.alias('source'),
    "target.guest_id = source.guest_id AND target.is_current = true"
).whenMatchedUpdate(
    set = {
        "is_current": "false",
        "valid_to": "current_timestamp()"
    }
).execute()
```

Step 3: Insert new versions
```python
new_versions.write.format('delta').mode('append').saveAsTable(table_name)
```

**Fact Table Incremental MERGE**:
```python
fact_stays_delta.alias('target').merge(
    fact_stays_final.alias('source'),
    'target.res_id = source.res_id'
).whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute()
```

#### Option 2: `bronze_to_silver_transformation.py` (STANDARD)

**Key Features**:
- Batch processing (no streaming)
- SCD Type 2 via window functions (lag/lead)
- Comprehensive data quality checks
- Simpler for understanding basic transformations

**SCD Type 2 Implementation (Window Functions)**:
```python
window_tier = Window.partitionBy('guest_id').orderBy(col('updated_at').asc_nulls_last())

guests_scd = (
    guests_deduped
    .withColumn('prev_tier', lag('loyalty_tier').over(window_tier))
    .withColumn('tier_changed', when((col('prev_tier') != col('loyalty_tier')), lit(1)).otherwise(lit(0)))
    .withColumn('version', 
        spark_sum('tier_changed').over(window_tier.rowsBetween(Window.unboundedPreceding, Window.currentRow)) + 1)
    .withColumn('valid_from', col('updated_at'))
    .withColumn('valid_to', lead('updated_at').over(window_tier))
    .withColumn('is_current', when(col('valid_to').isNull(), lit(True)).otherwise(lit(False)))
)
```

---

## Data Transformations

### 1. Guest Dimension (dim_guests)

**Transformations Applied**:

| Step | Transformation | Code Example |
|------|----------------|--------------|
| 1. **Clean Names** | Remove extra spaces, normalize case | `regexp_replace(trim(col('name')), '\\s+', ' ')` |
| 2. **Clean Emails** | Lowercase, remove invalid | `lower(trim(col('email')))` |
| 3. **Standardize Tiers** | Bronze/Silver/Gold/Platinum | `when(upper(col('loyalty_tier')) == 'BRONZE', 'Bronze')` |
| 4. **Parse Dates** | Handle multiple formats | `coalesce(try_to_date(..., 'yyyy-MM-dd'), try_to_date(..., 'dd/MM/yyyy'))` |
| 5. **Deduplicate** | Email base extraction | `regexp_replace(col('email'), '\\+.*@', '@')` |
| 6. **SCD Type 2** | Version history tracking | `lag/lead` or `MERGE` operations |

**Deduplication Logic**:
```python
# Create dedup key: email base (before +) or guest_id
email_base = regexp_replace(email, '\\+.*@', '@')  # email+alt@domain → email@domain
dedup_key = coalesce(email_base, concat_ws('_', 'guest', guest_id))

# Keep latest record per dedup_key
window = Window.partitionBy('dedup_key').orderBy(col('updated_at').desc_nulls_last())
guests_deduped = guests.withColumn('rn', row_number().over(window)).filter(col('rn') == 1)
```

### 2. Stays Fact Table (fact_stays_unified)

**Transformations Applied**:

| Step | Transformation | Purpose |
|------|----------------|---------|
| 1. **Clean Reservations** | Format standardization | Consistent room_type, booking_channel |
| 2. **Parse Dates** | Multiple format handling | check_in_date, check_out_date parsing |
| 3. **Validate Prices** | Filter outliers | Remove negative or extreme prices |
| 4. **Deduplicate** | Keep latest by created_at | One record per res_id |
| 5. **Clean POS** | Standardize categories | Food/Drink/Service/Spa |
| 6. **Aggregate POS** | Sum by res_id | total_pos_amount, pos_item_count |
| 7. **Join & Calculate** | Combine datasets | total_folio_amount = total_price + total_pos_amount |
| 8. **Filter Orphans** | Inner join with dimensions | Remove invalid guest_id/hotel_id |

**Late Checkout Logic (Bronze Data)**:
```python
# 12% of POS transactions occur AFTER checkout
if checkout_date and random.random() < 0.12:
    txn_timestamp = checkout_date + timedelta(hours=random.randint(1, 6))
```

**Dynamic Pricing Detection (Bronze Data)**:
```python
# Weekend check-ins have 2x pricing
is_weekend = check_in_date_obj.weekday() >= 5
total_price = base_price * 2.0 if is_weekend else base_price
```

### 3. Room Availability Fact Table (fact_room_availability_daily)

**Date Explosion Logic**:
```python
# Convert stays into daily records
room_daily = (
    reservations
    .withColumn('date_array', 
        expr('sequence(check_in_date, date_sub(check_out_date, 1), interval 1 day)'))
    .withColumn('date', explode(col('date_array')))
)
# Result: One row per night of stay
# Example: 3-night stay → 3 rows (check_in, check_in+1, check_in+2)
```

**Overbooking Detection & Resolution**:
```python
# Detect: Count bookings per (hotel_id, room_number, date)
window_overbooking = Window.partitionBy('hotel_id', 'room_number', 'date')
room_flagged = room_daily.withColumn('booking_count', count('res_id').over(window_overbooking))
room_flagged = room_flagged.withColumn('is_overbooked', when(col('booking_count') > 1, True).otherwise(False))

# Resolve: Keep first reservation (by res_id)
window_resolve = Window.partitionBy('hotel_id', 'room_number', 'date').orderBy('res_id')
fact_room_avail = room_flagged.withColumn('rn', row_number().over(window_resolve)).filter(col('rn') == 1)
```

---

## KPI Formulas

### 1. RevPAR (Revenue Per Available Room)

```sql
-- Daily RevPAR per hotel
SELECT 
    date,
    hotel_id,
    total_room_revenue,
    total_available_rooms,
    ROUND(total_room_revenue / total_available_rooms, 2) AS revpar
FROM (
    -- Step 1: Total revenue per date/hotel (explode stays into daily revenue)
    SELECT 
        date,
        hotel_id,
        SUM(total_price / stay_length_nights) AS total_room_revenue
    FROM fact_stays_unified
    LATERAL VIEW explode(sequence(check_in_date, date_sub(check_out_date, 1), interval 1 day)) AS date
    GROUP BY date, hotel_id
) revenue
JOIN (
    -- Step 2: Count unique rooms per date/hotel
    SELECT date, hotel_id, COUNT(DISTINCT room_number) AS total_available_rooms
    FROM fact_room_availability_daily
    GROUP BY date, hotel_id
) rooms
USING (date, hotel_id)
```

**Business Interpretation**:
- **High RevPAR** = Good performance (strong pricing + occupancy)
- **Low RevPAR** = Need to improve (either raise prices or increase occupancy)

### 2. ADR (Average Daily Rate)

```sql
-- Daily ADR per hotel
SELECT 
    date,
    hotel_id,
    total_room_revenue,
    rooms_sold,
    ROUND(total_room_revenue / rooms_sold, 2) AS adr
FROM (
    SELECT 
        date,
        hotel_id,
        SUM(total_price / stay_length_nights) AS total_room_revenue,
        COUNT(*) AS rooms_sold  -- Each exploded row = 1 room-night sold
    FROM fact_stays_unified
    LATERAL VIEW explode(sequence(check_in_date, date_sub(check_out_date, 1), interval 1 day)) AS date
    GROUP BY date, hotel_id
)
```

**Business Interpretation**:
- **High ADR** = Strong pricing power
- **Low ADR** = Maybe discounting too much

**Relationship**: `RevPAR = ADR × Occupancy Rate`

### 3. Ancillary Attachment Rate

```sql
-- Daily attachment rate per hotel
SELECT 
    date,
    hotel_id,
    total_guests,
    guests_with_ancillary_spend,
    ROUND((guests_with_ancillary_spend / total_guests) * 100, 2) AS attachment_rate_percentage,
    ROUND(total_ancillary_revenue / total_guests, 2) AS avg_ancillary_per_guest
FROM (
    SELECT 
        date,
        hotel_id,
        COUNT(*) AS total_guests,
        SUM(CASE WHEN pos_item_count > 0 THEN 1 ELSE 0 END) AS guests_with_ancillary_spend,
        SUM(total_pos_amount) AS total_ancillary_revenue
    FROM fact_stays_unified
    LATERAL VIEW explode(sequence(check_in_date, date_sub(check_out_date, 1), interval 1 day)) AS date
    GROUP BY date, hotel_id
)
```

**Business Interpretation**:
- **Target**: >60% attachment rate
- **Low Rate**: Improve amenity promotions
- **High Rate**: Strong guest engagement

### 4. Housekeeping Turnover Time

```sql
-- Average turnover time per date/hotel
SELECT 
    check_out_date AS date,
    hotel_id,
    COUNT(*) AS total_checkouts,
    ROUND(AVG(turnover_time_minutes), 2) AS avg_turnover_time_minutes,
    PERCENTILE_APPROX(turnover_time_minutes, 0.5) AS median_turnover_time_minutes
FROM (
    SELECT 
        r.check_out_date,
        r.hotel_id,
        (UNIX_TIMESTAMP(h.timestamp) - UNIX_TIMESTAMP(r.checkout_timestamp)) / 60 AS turnover_time_minutes
    FROM (
        SELECT *, 
               TIMESTAMP(CONCAT(check_out_date, ' 11:00:00')) AS checkout_timestamp
        FROM reservations
    ) r
    INNER JOIN housekeeping_logs h
    ON r.hotel_id = h.hotel_id AND r.room_number = h.room_number
    WHERE h.status = 'Clean'
      AND h.timestamp >= r.checkout_timestamp
      AND h.timestamp <= r.checkout_timestamp + INTERVAL 1 DAY
      AND (h.timestamp - r.checkout_timestamp) > 0
      AND (h.timestamp - r.checkout_timestamp) <= INTERVAL 10 HOURS
)
GROUP BY check_out_date, hotel_id
```

**Business Interpretation**:
- **Target**: <120 minutes (2 hours)
- **High Turnover**: Staffing or process issues
- **Low Turnover**: Efficient operations

### 5. Weekend vs Weekday Revenue

```sql
-- Compare weekend vs weekday metrics
SELECT 
    date,
    hotel_id,
    CASE WHEN DAYOFWEEK(date) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    adr,
    revpar,
    ROUND((rooms_sold / total_available_rooms) * 100, 2) AS occupancy_rate
FROM (
    SELECT 
        date,
        hotel_id,
        SUM(total_price / stay_length_nights) AS total_room_revenue,
        COUNT(*) AS rooms_sold,
        MAX(total_rooms) AS total_available_rooms,
        ROUND(SUM(total_price / stay_length_nights) / COUNT(*), 2) AS adr,
        ROUND(SUM(total_price / stay_length_nights) / MAX(total_rooms), 2) AS revpar
    FROM fact_stays_unified
    LATERAL VIEW explode(sequence(check_in_date, date_sub(check_out_date, 1), interval 1 day)) AS date
    JOIN (
        SELECT date, hotel_id, COUNT(DISTINCT room_number) AS total_rooms
        FROM fact_room_availability_daily
        GROUP BY date, hotel_id
    ) USING (date, hotel_id)
    GROUP BY date, hotel_id
)
```

**Business Interpretation**:
- **Expected**: Weekend ADR ≈ 2× Weekday ADR
- **Analysis**: Validate dynamic pricing strategy

---

## Performance Optimization

### 1. Partitioning Strategy

```python
# Silver Layer
fact_stays_unified: partitionBy('check_in_date')  # Query filter: check_in_date range
fact_room_availability_daily: partitionBy('date') # Query filter: date range

# Gold Layer
All KPI tables: partitionBy('date')  # Time-series queries
```

**Benefits**:
- **Partition pruning**: Read only relevant files
- **Faster queries**: Especially for date range filters

### 2. Delta Lake Optimizations

```sql
-- OPTIMIZE (compaction)
OPTIMIZE hospitality_project.silver_schema.fact_stays_unified
ZORDER BY (guest_id, hotel_id);

-- VACUUM (cleanup old files after 7 days)
VACUUM hospitality_project.silver_schema.fact_stays_unified RETAIN 168 HOURS;

-- Table properties
ALTER TABLE hospitality_project.silver_schema.fact_stays_unified
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

### 3. Checkpoint Management

```python
# Auto Loader checkpoints track:
# - Files already processed
# - Schema evolution history
# - Offset tracking

# Location: /Volumes/hospitality_project/bronze_schema/raw/_checkpoints/
# - dim_guests_checkpoint/
# - fact_stays_checkpoint/

# To reset (reprocess all files):
dbutils.fs.rm("/Volumes/.../dim_guests_checkpoint", recurse=True)
```

### 4. Broadcast Joins

```python
# For small dimension tables (<10MB)
valid_guests = spark.table("dim_guests").select('guest_id').distinct()
fact_stays = fact_stays.join(broadcast(valid_guests), on='guest_id', how='inner')
```

---

## Troubleshooting

### Common Issues & Solutions

#### 1. "Table not found" Error

**Symptom**: `pyspark.sql.utils.AnalysisException: Table or view not found`

**Solution**:
```sql
-- Check catalog exists
SHOW CATALOGS LIKE 'hospitality_project';

-- Check schema exists
SHOW SCHEMAS IN hospitality_project;

-- Check table exists
SHOW TABLES IN hospitality_project.silver_schema;
```

#### 2. Schema Evolution Errors

**Symptom**: `AnalysisException: A schema mismatch detected when writing to ...`

**Solution**:
```python
# Enable schema merge
df.write.format('delta') \
    .mode('overwrite') \
    .option('mergeSchema', 'true') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(table_name)
```

#### 3. SCD Type 2 Not Creating Versions

**Symptom**: All guests have version = 1

**Solution**:
- Check `updated_at` is incrementing over time
- Verify `loyalty_tier` actually changes between records
- Run second batch of data with tier changes

#### 4. Checkpoint Corruption

**Symptom**: `java.io.FileNotFoundException: ... _checkpoint/...`

**Solution**:
```python
# Delete and recreate checkpoint
dbutils.fs.rm("/Volumes/.../dim_guests_checkpoint", recurse=True)
# Rerun the notebook
```

#### 5. Low KPI Values

**Symptom**: RevPAR is $0 or very low

**Solution**:
- Check date ranges align between revenue and room counts
- Verify `total_available_rooms > 0`
- Confirm date explosion logic is correct

---

## Maintenance Guide

### Daily Operations

1. **Monitor Pipeline Health**
```sql
-- Check latest data ingestion
SELECT MAX(created_at) as latest_load
FROM hospitality_project.silver_schema.fact_stays_unified;
```

2. **Validate Data Quality**
```sql
-- Check for anomalies
SELECT date, hotel_id, revpar
FROM hospitality_project.gold_schema.kpi_revpar
WHERE revpar > 1000 OR revpar < 0;  -- Outliers
```

3. **Optimize Tables** (Weekly)
```sql
OPTIMIZE hospitality_project.silver_schema.fact_stays_unified;
OPTIMIZE hospitality_project.gold_schema.kpi_revpar;
```

4. **Vacuum Old Files** (Monthly)
```sql
VACUUM hospitality_project.silver_schema.fact_stays_unified RETAIN 168 HOURS;
```

---

## Security & Governance

### Unity Catalog Permissions

```sql
-- Grant read access to analysts
GRANT SELECT ON SCHEMA hospitality_project.gold_schema TO analyst_group;

-- Grant write access to data engineers
GRANT ALL PRIVILEGES ON SCHEMA hospitality_project.silver_schema TO data_engineer_group;

-- Row-level security (example)
CREATE ROW ACCESS POLICY hotel_access_policy
AS (hotel_id INT) -> current_user() IN (
    SELECT user_name FROM access_control WHERE allowed_hotel_id = hotel_id
);
```

---

## Appendix: Complete Schema Definitions

### Silver Layer DDL

```sql
-- dim_guests
CREATE TABLE hospitality_project.silver_schema.dim_guests (
    guest_sk STRING,
    guest_id INT,
    name STRING,
    email STRING,
    loyalty_tier STRING,
    country STRING,
    registration_date DATE,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN,
    version INT
) USING DELTA;

-- fact_stays_unified
CREATE TABLE hospitality_project.silver_schema.fact_stays_unified (
    res_id INT,
    guest_id INT,
    hotel_id INT,
    room_type STRING,
    check_in_date DATE,
    check_out_date DATE,
    stay_length_nights INT,
    total_price DOUBLE,
    booking_channel STRING,
    total_pos_amount DOUBLE,
    total_folio_amount DOUBLE,
    pos_item_count INT,
    created_at TIMESTAMP
) USING DELTA
PARTITIONED BY (check_in_date);

-- fact_room_availability_daily
CREATE TABLE hospitality_project.silver_schema.fact_room_availability_daily (
    date DATE,
    hotel_id INT,
    room_number STRING,
    room_type STRING,
    is_available BOOLEAN,
    res_id INT,
    guest_id INT,
    check_in_date DATE,
    check_out_date DATE,
    is_overbooked BOOLEAN
) USING DELTA
PARTITIONED BY (date);
```

### Gold Layer DDL

```sql
-- kpi_revpar
CREATE TABLE hospitality_project.gold_schema.kpi_revpar (
    date DATE,
    hotel_id INT,
    total_room_revenue DOUBLE,
    total_available_rooms INT,
    revpar DOUBLE
) USING DELTA
PARTITIONED BY (date);

-- kpi_adr
CREATE TABLE hospitality_project.gold_schema.kpi_adr (
    date DATE,
    hotel_id INT,
    total_room_revenue DOUBLE,
    rooms_sold INT,
    adr DOUBLE
) USING DELTA
PARTITIONED BY (date);

-- kpi_ancillary_attachment_rate
CREATE TABLE hospitality_project.gold_schema.kpi_ancillary_attachment_rate (
    date DATE,
    hotel_id INT,
    total_guests INT,
    guests_with_ancillary_spend INT,
    attachment_rate_percentage DOUBLE,
    total_ancillary_revenue DOUBLE,
    avg_ancillary_per_guest DOUBLE
) USING DELTA
PARTITIONED BY (date);

-- kpi_housekeeping_turnover_time
CREATE TABLE hospitality_project.gold_schema.kpi_housekeeping_turnover_time (
    date DATE,
    hotel_id INT,
    total_checkouts INT,
    avg_turnover_time_minutes DOUBLE,
    median_turnover_time_minutes DOUBLE,
    min_turnover_time_minutes DOUBLE,
    max_turnover_time_minutes DOUBLE
) USING DELTA
PARTITIONED BY (date);

-- kpi_weekend_vs_weekday_revenue
CREATE TABLE hospitality_project.gold_schema.kpi_weekend_vs_weekday_revenue (
    date DATE,
    hotel_id INT,
    is_weekend BOOLEAN,
    adr DOUBLE,
    revpar DOUBLE,
    occupancy_rate DOUBLE,
    rooms_sold INT,
    total_available_rooms INT,
    total_room_revenue DOUBLE
) USING DELTA
PARTITIONED BY (date);

-- kpi_executive_dashboard
CREATE TABLE hospitality_project.gold_schema.kpi_executive_dashboard (
    date DATE,
    hotel_id INT,
    revpar DOUBLE,
    adr DOUBLE,
    attachment_rate_percentage DOUBLE,
    avg_ancillary_per_guest DOUBLE
) USING DELTA;
```

---

**End of Technical Documentation**

*For questions, refer to README.md or examine the source code in `B2S/` folder.*

*Shri Radhe Govind Ji! 🙏*
