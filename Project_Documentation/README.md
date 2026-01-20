# Hospitality Data Engineering Project 🏨

**Shri Radhe Govind Ji! 🙏**

## 📋 Project Overview

This is a comprehensive **end-to-end data engineering solution** for the hospitality industry, built on **Databricks** using **Delta Lake** and **Unity Catalog**. The project implements a modern **medallion architecture** (Bronze → Silver → Gold) to transform raw hospitality data into actionable business intelligence.

### Business Context

The solution addresses real-world challenges in hotel management:
- **Revenue Management**: Track RevPAR, ADR, and pricing strategies
- **Operations**: Monitor housekeeping efficiency and room availability
- **Guest Experience**: Analyze ancillary spending patterns and loyalty tiers
- **Data Quality**: Handle messy real-world data with transformations and validations

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER (Raw Data)                      │
├─────────────────────────────────────────────────────────────────────┤
│  • raw_guests (~5,150 records)          • raw_reservations (~8,150) │
│  • raw_hotel_inventory (~2,000)         • raw_pos_transactions      │
│  • raw_housekeeping_logs (~12,150)        (~15,200)                 │
│                                                                       │
│  Data Issues: Duplicates, Missing Values, Format Inconsistencies,   │
│              Overbooking, Late Checkout Transactions                │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
        ┌───────────────────────────────────────────────┐
        │    TRANSFORMATION LAYER (Auto Loader + SCD)   │
        └───────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│                      SILVER LAYER (Clean Data)                      │
├─────────────────────────────────────────────────────────────────────┤
│  • dim_guests (SCD Type 2 - Loyalty Tier Tracking)                  │
│  • fact_stays_unified (Reservations + POS Transactions)             │
│  • fact_room_availability_daily (Date-Exploded Room Calendar)       │
│                                                                       │
│  Features: Deduplication, Format Standardization, Orphan Removal,   │
│           SCD Type 2 with MERGE, Overbooking Detection              │
└─────────────────────────────────────────────────────────────────────┘
                                    ↓
        ┌───────────────────────────────────────────────┐
        │         GOLD LAYER (Business KPIs)            │
        └───────────────────────────────────────────────┘
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER (Business KPIs)                       │
├─────────────────────────────────────────────────────────────────────┤
│  1. kpi_revpar - Revenue Per Available Room                         │
│  2. kpi_adr - Average Daily Rate                                    │
│  3. kpi_ancillary_attachment_rate - Guest Ancillary Spend           │
│  4. kpi_housekeeping_turnover_time - Room Cleaning Efficiency       │
│  5. kpi_weekend_vs_weekday_revenue - Dynamic Pricing Validation     │
│  6. kpi_executive_dashboard - Unified Executive View                │
└─────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Key Features

### 🎯 Advanced Data Engineering Techniques

1. **Auto Loader with Cloud Files**
   - Incremental file processing
   - Schema evolution support
   - Rescued data column for malformed records
   - Checkpoint management for state tracking

2. **Slowly Changing Dimension Type 2 (SCD Type 2)**
   - Track loyalty tier changes over time
   - MERGE operations for incremental updates
   - Valid from/to timestamps
   - Current vs historical record flags

3. **Data Quality Transformations**
   - Deduplication (guest email variations, exact duplicates)
   - Format standardization (dates, timestamps, categorical values)
   - Outlier detection and filtering
   - Orphaned record removal
   - Overbooking detection and resolution

4. **Streaming with Trigger-Once**
   - Batch-style processing with streaming benefits
   - Exactly-once semantics
   - State management via checkpoints

### 📊 Business Intelligence KPIs

#### 1. **RevPAR** (Revenue Per Available Room)
- **Formula**: Total Room Revenue / Total Available Rooms
- **Purpose**: Industry benchmark for hotel performance
- **Use Case**: Compare performance across hotels

#### 2. **ADR** (Average Daily Rate)
- **Formula**: Total Room Revenue / Rooms Sold
- **Purpose**: Measure pricing effectiveness
- **Use Case**: Optimize pricing strategy

#### 3. **Ancillary Attachment Rate**
- **Formula**: (Guests with POS Spend / Total Guests) × 100
- **Purpose**: Track guest engagement with amenities
- **Use Case**: Design package offers and promotions

#### 4. **Housekeeping Turnover Time**
- **Formula**: AVG(Time between Checkout and Clean Status)
- **Purpose**: Operational efficiency metric
- **Use Case**: Optimize housekeeping schedules

#### 5. **Weekend vs Weekday Revenue**
- **Metrics**: ADR, RevPAR, Occupancy by day type
- **Purpose**: Validate dynamic pricing strategy
- **Use Case**: Adjust pricing calendar

---

## 📁 Project Structure

```
Hospitality_DE_Project/
│
├── B2S/                                    # Solution Files
│   ├── bronze_to_silver_auto_loader.py    # Auto Loader + SCD Type 2 (MERGE-based)
│   ├── bronze_to_silver_transformation.py  # Standard B2S transformation
│   └── Silver_to_Gold (1).py              # KPI calculations
│
├── 05_hospitality_data_setup.py           # Bronze data generation
├── 05_hospitality_requirement.py          # Project requirements
│
└── Project_Documentation/                  # THIS FOLDER - Documentation
    ├── README.md                           # This file
    ├── Technical_Documentation.md          # Detailed technical guide
    └── Hospitality_DE_Project_Presentation.md  # Presentation outline
```

---

## 🚀 Getting Started

### Prerequisites

- **Databricks Workspace** (Unity Catalog enabled)
- **Python 3.9+**
- **PySpark 3.4+**
- **Faker library** (for data generation)

### Setup Instructions

#### Step 1: Create Bronze Data

Run the data setup notebook to generate bronze layer data:

```python
# Execute: 05_hospitality_data_setup.py
# This creates:
# - Catalog: hospitality_project
# - Schema: bronze_schema
# - Volume: raw
# - Sample data files with intentional data quality issues
```

#### Step 2: Transform to Silver Layer

Run **either** of the following notebooks (both produce the same silver tables):

**Option A: Auto Loader with MERGE (Recommended)**
```python
# Execute: B2S/bronze_to_silver_auto_loader.py
# Features:
# - Auto Loader with schema evolution
# - TRUE SCD Type 2 with MERGE operations
# - Incremental processing
# - Checkpoint management
```

**Option B: Standard Transformation**
```python
# Execute: B2S/bronze_to_silver_transformation.py
# Features:
# - Batch processing
# - SCD Type 2 with window functions
# - Comprehensive data quality checks
```

#### Step 3: Create Gold Layer KPIs

```python
# Execute: B2S/Silver_to_Gold (1).py
# Creates all business KPIs and executive dashboard
```

---

## 📊 Data Model

### Silver Layer Tables

#### 1. dim_guests (Dimension - SCD Type 2)

| Column             | Type      | Description                          |
|--------------------|-----------|--------------------------------------|
| guest_sk           | STRING    | Surrogate key (guest_id_version)     |
| guest_id           | INT       | Natural key                          |
| name               | STRING    | Guest name                           |
| email              | STRING    | Email address                        |
| loyalty_tier       | STRING    | Bronze/Silver/Gold/Platinum          |
| country            | STRING    | Guest country                        |
| registration_date  | DATE      | Account registration date            |
| valid_from         | TIMESTAMP | SCD Type 2 start timestamp           |
| valid_to           | TIMESTAMP | SCD Type 2 end timestamp (NULL=current) |
| is_current         | BOOLEAN   | Current version flag                 |
| version            | INT       | Version number                       |

#### 2. fact_stays_unified (Fact Table)

| Column               | Type      | Description                       |
|----------------------|-----------|-----------------------------------|
| res_id               | INT       | Reservation ID (PK)               |
| guest_id             | INT       | FK to dim_guests                  |
| hotel_id             | INT       | Hotel identifier                  |
| room_type            | STRING    | Standard/Deluxe/Suite/Presidential |
| check_in_date        | DATE      | Check-in date                     |
| check_out_date       | DATE      | Check-out date                    |
| stay_length_nights   | INT       | Number of nights                  |
| total_price          | DOUBLE    | Room revenue                      |
| booking_channel      | STRING    | Website/Phone/Walk-in/OTA         |
| total_pos_amount     | DOUBLE    | Total ancillary spend             |
| total_folio_amount   | DOUBLE    | Total revenue (room + ancillary)  |
| pos_item_count       | INT       | Number of ancillary items         |
| created_at           | TIMESTAMP | Booking timestamp                 |

#### 3. fact_room_availability_daily (Fact Table)

| Column          | Type      | Description                    |
|-----------------|-----------|--------------------------------|
| date            | DATE      | Calendar date (PK part)        |
| hotel_id        | INT       | Hotel identifier (PK part)     |
| room_number     | STRING    | Room number (PK part)          |
| room_type       | STRING    | Room category                  |
| is_available    | BOOLEAN   | Availability flag (always FALSE in this data) |
| res_id          | INT       | Reservation ID                 |
| guest_id        | INT       | Guest ID                       |
| check_in_date   | DATE      | Reservation check-in           |
| check_out_date  | DATE      | Reservation check-out          |
| is_overbooked   | BOOLEAN   | Overbooking flag               |

### Gold Layer Tables

All gold tables are aggregated by **date** and **hotel_id**. See [Technical Documentation](./Technical_Documentation.md) for detailed schema and formulas.

---

## 🎓 Learning Objectives

This project demonstrates:

1. **Medallion Architecture** (Bronze-Silver-Gold)
2. **Auto Loader** for incremental data ingestion
3. **SCD Type 2** dimension modeling
4. **MERGE operations** for incremental updates
5. **Data quality** transformations
6. **Date explosion** for calendar tables
7. **Window functions** for analytics
8. **Hospitality industry** KPIs
9. **Unity Catalog** best practices
10. **Delta Lake** optimization

---

## 📈 Sample Queries

### Get Current Guest Loyalty Tiers
```sql
SELECT guest_id, name, email, loyalty_tier
FROM hospitality_project.silver_schema.dim_guests
WHERE is_current = TRUE
ORDER BY loyalty_tier DESC, guest_id;
```

### Revenue by Hotel
```sql
SELECT hotel_id, 
       COUNT(DISTINCT res_id) as total_reservations,
       ROUND(SUM(total_folio_amount), 2) as total_revenue,
       ROUND(AVG(total_folio_amount), 2) as avg_revenue_per_stay
FROM hospitality_project.silver_schema.fact_stays_unified
GROUP BY hotel_id
ORDER BY total_revenue DESC;
```

### RevPAR Trend Analysis
```sql
SELECT date, 
       ROUND(AVG(revpar), 2) as avg_revpar_all_hotels
FROM hospitality_project.gold_schema.kpi_revpar
GROUP BY date
ORDER BY date;
```

---

## 🔍 Data Quality Issues Addressed

The bronze data contains **intentional** data quality issues that the pipeline resolves:

| Issue Type                  | Examples                                | Resolution                     |
|-----------------------------|-----------------------------------------|--------------------------------|
| **Duplicates**              | Same guest with email variations        | Deduplication by email base    |
| **Missing Values**          | NULL loyalty_tier, email, dates         | Default values or filtering    |
| **Format Inconsistencies**  | Multiple date formats (dd/MM/yyyy, etc.)| Multiple format parsing        |
| **Data Type Issues**        | Strings for numbers, wrong timestamps   | Type casting and validation    |
| **Orphaned Records**        | Reservations with non-existent guests   | Inner join filtering           |
| **Overbooking**             | Same room booked twice on same date     | Detection + resolution logic   |
| **Outliers**                | Negative prices, extreme values         | Range filtering               |
| **Late Checkout**           | POS transactions after checkout         | Timestamp validation           |

---

## 🤝 Contributing

This project is designed for learning and demonstration. Feel free to:
- Extend with additional KPIs
- Add data visualization dashboards
- Implement real-time streaming
- Add automated testing

---

## 📞 Support

For questions or issues:
1. Review the [Technical Documentation](./Technical_Documentation.md)
2. Check the requirements file: `05_hospitality_requirement.py`
3. Examine the actual code in the `B2S/` folder

---

## 📜 License

This project is for educational purposes. 

---

**Created with ❤️ for Data Engineering Excellence**

*Shri Radhe Govind Ji! 🙏*
