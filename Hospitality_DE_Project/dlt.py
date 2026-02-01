import dlt
from pyspark.sql.functions import (
    col, when, trim, upper, lower, regexp_replace, to_date,
    coalesce, lit, current_timestamp, row_number, dense_rank, lag, lead,
    datediff, explode, sequence, date_add, sum as spark_sum, count, avg,
    max as spark_max, min as spark_min, expr, concat_ws, md5, unix_timestamp,
    dayofweek, year, month, dayofmonth, date_sub, countDistinct,
    percentile_approx, to_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, TimestampType

# Configuration
CATALOG_NAME = 'hospitality_project'
BRONZE_SCHEMA = 'bronze_schema'
VOLUME_NAME = 'raw'
VOLUME_PATH = f'/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/{VOLUME_NAME}'

print(f"📂 Volume Path: {VOLUME_PATH}")

# COMMAND ----------

# 🥉 BRONZE LAYER - Auto Loader Ingestion
#  Raw data ingestion with schema evolution and rescued data column.

# COMMAND ----------

# Bronze: Guests
@dlt.table(
    name="bronze_guests",
    comment="Raw guest data with Auto Loader - includes duplicates and data quality issues",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "guest_id"
    }
)
def bronze_guests():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{VOLUME_PATH}/guests")
    )

# COMMAND ----------

# Bronze: Hotel Inventory
@dlt.table(
    name="bronze_hotel_inventory",
    comment="Raw hotel room inventory data",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "hotel_id,room_number"
    }
)
def bronze_hotel_inventory():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{VOLUME_PATH}/hotel_inventory")
    )

# COMMAND ----------

# Bronze: Reservations
@dlt.table(
    name="bronze_reservations",
    comment="Raw reservation data with overbooking and dynamic pricing",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "res_id"
    }
)
def bronze_reservations():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{VOLUME_PATH}/reservations")
    )

# COMMAND ----------

# Bronze: POS Transactions
@dlt.table(
    name="bronze_pos_transactions",
    comment="Raw point-of-sale transaction data with late checkout logic",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "txn_id"
    }
)
def bronze_pos_transactions():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{VOLUME_PATH}/pos_transactions")
    )

# COMMAND ----------

# Bronze: Housekeeping Logs
@dlt.table(
    name="bronze_housekeeping_logs",
    comment="Raw housekeeping status logs",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "hotel_id,room_number"
    }
)
def bronze_housekeeping_logs():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{VOLUME_PATH}/housekeeping_logs")
    )

# COMMAND ----------

# 🥈 SILVER LAYER - Cleaned and Conformed Data
# Part 1: dim_guests (SCD Type 2)

# COMMAND ----------

@dlt.table(
    name="silver_guests_cleaned",
    comment="Cleaned guest data - intermediate step before SCD Type 2",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "guest_id"
    }
)
@dlt.expect_all({
    "valid_guest_id": "guest_id IS NOT NULL",
    "valid_name": "name IS NOT NULL AND length(name) > 0",
    "valid_updated_at": "updated_at IS NOT NULL"
})
def silver_guests_cleaned():
    """Clean and standardize guest data"""
    
    return (
        dlt.read_stream("bronze_guests")
        
        # Filter invalid records
        .filter(col('guest_id').isNotNull())
        
        # Clean name
        .withColumn('name', 
            when(col('name').isNotNull(), 
                 regexp_replace(trim(col('name')), '\\s+', ' '))
            .otherwise(lit('Unknown')))
        
        # Clean email
        .withColumn('email',
            when(col('email').isNotNull() & (col('email') != 'invalid-email'),
                 lower(trim(col('email'))))
            .otherwise(None))
        
        # Standardize loyalty_tier
        .withColumn('loyalty_tier',
            when(col('loyalty_tier').isNotNull(),
                 when(upper(col('loyalty_tier')) == 'BRONZE', 'Bronze')
                 .when(upper(col('loyalty_tier')) == 'SILVER', 'Silver')
                 .when(upper(col('loyalty_tier')) == 'GOLD', 'Gold')
                 .when(upper(col('loyalty_tier')) == 'PLATINUM', 'Platinum')
                 .otherwise('Bronze'))
            .otherwise('Bronze'))
        
        # Clean country
        .withColumn('country',
            when(col('country').isNotNull(), upper(trim(col('country'))))
            .otherwise(lit('UNKNOWN')))
        
        # Parse registration_date
        .withColumn('registration_date',
            coalesce(
                to_date(col('registration_date'), 'yyyy-MM-dd'),
                to_date(col('registration_date'), 'dd/MM/yyyy'),
                to_date(col('registration_date'), 'MM/dd/yyyy')
            ))
        
        # Parse updated_at
        .withColumn('updated_at',
            coalesce(
                to_timestamp(col('updated_at'), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                to_timestamp(col('updated_at'), 'dd/MM/yyyy HH:mm'),
                to_timestamp(col('updated_at'), 'yyyy-MM-dd HH:mm:ss')
            ))
        
        # Filter records with valid updated_at (required for SCD Type 2)
        .filter(col('updated_at').isNotNull())
        
        # Deduplication - create dedup key
        .withColumn('email_base',
            when(col('email').isNotNull(),
                 regexp_replace(col('email'), '\\+.*@', '@'))
            .otherwise(None))
        .withColumn('dedup_key',
            coalesce(
                col('email_base'),
                concat_ws('_', lit('guest'), col('guest_id'))
            ))
    )

# COMMAND ----------

@dlt.table(
    name="silver_guests_deduped",
    comment="Deduplicated guest data - keeps latest record per unique guest",
    table_properties={
        "quality": "silver"
    }
)
def silver_guests_deduped():
    """Deduplicate guests - keep latest record per email"""
    
    window_dedup = Window.partitionBy('dedup_key').orderBy(
        col('updated_at').desc_nulls_last(),
        col('guest_id').desc()
    )
    
    return (
        dlt.read("silver_guests_cleaned")
        .withColumn('rn', row_number().over(window_dedup))
        .filter(col('rn') == 1)
        .drop('rn', 'email_base', 'dedup_key', '_rescued_data')
        .select(
            'guest_id', 'name', 'email', 'loyalty_tier', 'country',
            'registration_date', 'updated_at'
        )
    )

# COMMAND ----------

@dlt.table(
    name="dim_guests",
    comment="Guest dimension with SCD Type 2 for loyalty tier changes",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "guest_id,is_current"
    }
)
@dlt.expect_all({
    "valid_guest_sk": "guest_sk IS NOT NULL",
    "valid_tier": "loyalty_tier IN ('Bronze', 'Silver', 'Gold', 'Platinum')",
    "valid_from_not_null": "valid_from IS NOT NULL"
})
def dim_guests():
    """Implement SCD Type 2 for loyalty tier tracking"""
    
    window_tier = Window.partitionBy('guest_id').orderBy(
        col('updated_at').asc_nulls_last()
    )
    
    return (
        dlt.read("silver_guests_deduped")
        
        # Detect tier changes
        .withColumn('prev_tier', lag('loyalty_tier').over(window_tier))
        .withColumn('tier_changed',
            when((col('prev_tier').isNotNull()) & (col('prev_tier') != col('loyalty_tier')), lit(1))
            .otherwise(lit(0)))
        
        # Create version number
        .withColumn('version',
            spark_sum('tier_changed').over(
                window_tier.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ) + 1)
        
        # Set validity dates
        .withColumn('valid_from', col('updated_at'))
        .withColumn('valid_to', lead('updated_at').over(window_tier))
        .withColumn('is_current', when(col('valid_to').isNull(), lit(True)).otherwise(lit(False)))
        
        # Create surrogate key
        .withColumn('guest_sk', concat_ws('_', col('guest_id').cast('string'), col('version').cast('string')))
        
        # Select final columns
        .select(
            'guest_sk', 'guest_id', 'name', 'email', 'loyalty_tier',
            'country', 'registration_date', 'valid_from', 'valid_to',
            'is_current', 'version'
        )
    )

# COMMAND ----------


# Part 2: fact_stays_unified

# COMMAND ----------

@dlt.table(
    name="silver_reservations_cleaned",
    comment="Cleaned reservation data",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_all({
    "valid_res_id": "res_id IS NOT NULL",
    "valid_dates": "check_in_date IS NOT NULL AND check_out_date IS NOT NULL",
    "logical_dates": "check_out_date > check_in_date"
})
def silver_reservations_cleaned():
    """Clean and standardize reservation data"""
    
    return (
        dlt.read_stream("bronze_reservations")
        
        .filter(col('res_id').isNotNull())
        
        # Standardize room_type
        .withColumn('room_type',
            when(col('room_type').isNotNull(),
                 when(upper(col('room_type')) == 'STANDARD', 'Standard')
                 .when(upper(col('room_type')) == 'DELUXE', 'Deluxe')
                 .when(upper(col('room_type')) == 'SUITE', 'Suite')
                 .when(upper(col('room_type')) == 'PRESIDENTIAL', 'Presidential')
                 .otherwise('Standard'))
            .otherwise('Standard'))
        
        # Standardize booking_channel
        .withColumn('booking_channel',
            when(col('booking_channel').isNotNull(),
                 when(upper(col('booking_channel')) == 'WEBSITE', 'Website')
                 .when(upper(col('booking_channel')) == 'PHONE', 'Phone')
                 .when(upper(col('booking_channel')).contains('WALK'), 'Walk-in')
                 .when(upper(col('booking_channel')) == 'OTA', 'OTA')
                 .otherwise('Unknown'))
            .otherwise('Unknown'))
        
        # Parse dates
        .withColumn('check_in_date',
            coalesce(
                to_date(col('check_in_date'), 'yyyy-MM-dd'),
                to_date(col('check_in_date'), 'dd/MM/yyyy'),
                to_date(col('check_in_date'), 'MM/dd/yyyy')
            ))
        .withColumn('check_out_date',
            coalesce(
                to_date(col('check_out_date'), 'yyyy-MM-dd'),
                to_date(col('check_out_date'), 'dd/MM/yyyy'),
                to_date(col('check_out_date'), 'MM-dd-yyyy'),
                to_date(col('check_out_date'), 'MM/dd/yyyy')
            ))
        .withColumn('created_at',
            coalesce(
                to_timestamp(col('created_at'), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                to_timestamp(col('created_at'), 'dd/MM/yyyy HH:mm'),
                to_timestamp(col('created_at'), 'yyyy-MM-dd HH:mm:ss')
            ))
        
        # Clean total_price
        .withColumn('total_price',
            when((col('total_price').isNotNull()) & 
                 (col('total_price') > 0) & 
                 (col('total_price') < 10000),
                 col('total_price'))
            .otherwise(None))
        
        # Filter valid records
        .filter(
            (col('check_in_date').isNotNull()) &
            (col('check_out_date').isNotNull()) &
            (col('check_out_date') > col('check_in_date'))
        )
    )

# COMMAND ----------

@dlt.table(
    name="silver_reservations_deduped",
    comment="Deduplicated reservations - keeps latest per res_id",
    table_properties={
        "quality": "silver"
    }
)
def silver_reservations_deduped():
    """Remove duplicate reservations"""
    
    window_res = Window.partitionBy('res_id').orderBy(col('created_at').desc_nulls_last())
    
    return (
        dlt.read("silver_reservations_cleaned")
        .withColumn('rn', row_number().over(window_res))
        .filter(col('rn') == 1)
        .drop('rn', '_rescued_data')
    )

# COMMAND ----------

@dlt.table(
    name="silver_pos_aggregated",
    comment="POS transactions aggregated by reservation",
    table_properties={
        "quality": "silver"
    }
)
def silver_pos_aggregated():
    """Aggregate POS transactions by reservation"""
    
    return (
        dlt.read_stream("bronze_pos_transactions")
        
        .filter(col('txn_id').isNotNull())
        
        # Standardize category
        .withColumn('category',
            when(col('category').isNotNull(),
                 when(upper(col('category')) == 'FOOD', 'Food')
                 .when(upper(col('category')) == 'DRINK', 'Drink')
                 .when(upper(col('category')) == 'SERVICE', 'Service')
                 .when(upper(col('category')) == 'SPA', 'Spa')
                 .otherwise('Other'))
            .otherwise('Other'))
        
        # Parse timestamp
        .withColumn('timestamp',
            coalesce(
                expr("try_to_timestamp(timestamp, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")"),
                expr("try_to_timestamp(timestamp, 'dd/MM/yyyy HH:mm')"),
                expr("try_to_timestamp(timestamp, 'yyyy-MM-dd HH:mm:ss')")
            ))
        
        # Clean amount
        .withColumn('amount',
            when((col('amount').isNotNull()) & 
                 (col('amount') > 0) & 
                 (col('amount') < 5000),
                 col('amount'))
            .otherwise(None))
        
        # Filter valid records
        .filter(
            (col('amount').isNotNull()) &
            (col('timestamp').isNotNull()) &
            (col('res_id').isNotNull())  # Only reservations, exclude walk-ins
        )
        
        # Aggregate by res_id
        .groupBy('res_id')
        .agg(
            spark_sum('amount').alias('total_pos_amount'),
            count('*').alias('pos_item_count')
        )
    )

# COMMAND ----------

@dlt.table(
    name="fact_stays_unified",
    comment="Unified reservation and POS fact table with total folio amounts",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "check_in_date,hotel_id"
    }
)
@dlt.expect_all({
    "valid_guest": "guest_id IS NOT NULL",
    "valid_hotel": "hotel_id IS NOT NULL",
    "valid_stay_length": "stay_length_nights > 0"
})
def fact_stays_unified():
    """Create unified stays fact table"""
    
    # Get valid guest_ids
    valid_guests = dlt.read("dim_guests").select('guest_id').distinct()
    
    # Get valid hotel_ids  
    valid_hotels = (
        dlt.read_stream("bronze_hotel_inventory")
        .select('hotel_id').distinct()
    )
    
    return (
        dlt.read("silver_reservations_deduped")
        
        # Join with POS aggregations
        .join(
            dlt.read("silver_pos_aggregated"),
            on='res_id',
            how='left'
        )
        
        # Fill nulls for reservations without POS
        .withColumn('total_pos_amount', coalesce(col('total_pos_amount'), lit(0.0)))
        .withColumn('pos_item_count', coalesce(col('pos_item_count'), lit(0)))
        
        # Calculate total folio
        .withColumn('total_folio_amount',
            when(col('total_price').isNotNull(),
                 col('total_price') + col('total_pos_amount'))
            .otherwise(col('total_pos_amount')))
        
        # Calculate stay length
        .withColumn('stay_length_nights', datediff(col('check_out_date'), col('check_in_date')))
        
        # Filter orphaned records
        .join(valid_guests, on='guest_id', how='inner')
        .join(valid_hotels, on='hotel_id', how='inner')
        
        # Select final columns
        .select(
            'res_id', 'guest_id', 'hotel_id', 'room_type',
            'check_in_date', 'check_out_date', 'stay_length_nights',
            'total_price', 'booking_channel', 'total_pos_amount',
            'total_folio_amount', 'pos_item_count', 'created_at'
        )
    )

# COMMAND ----------

# Part 3: fact_room_availability_daily

# COMMAND ----------

@dlt.table(
    name="fact_room_availability_daily",
    comment="Daily room availability calendar with overbooking detection",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "date,hotel_id"
    }
)
@dlt.expect_all({
    "valid_date": "date IS NOT NULL",
    "valid_room": "room_number IS NOT NULL AND room_number != '9999'"
})
def fact_room_availability_daily():
    """Create daily room availability with date explosion"""
    
    # Get reservations with valid room assignments
    reservations_with_rooms = (
        dlt.read("silver_reservations_deduped")
        .filter(col('room_number').isNotNull())
        .filter(col('room_number') != '9999')
        .select(
            'res_id', 'guest_id', 'hotel_id', 'room_number',
            'room_type', 'check_in_date', 'check_out_date'
        )
    )
    
    # Date explosion
    room_daily = (
        reservations_with_rooms
        .withColumn('date_array',
            expr('sequence(check_in_date, date_sub(check_out_date, 1), interval 1 day)'))
        .withColumn('date', explode(col('date_array')))
        .withColumn('is_available', lit(False))
        .select(
            'date', 'hotel_id', 'room_number', 'room_type',
            'is_available', 'res_id', 'guest_id',
            'check_in_date', 'check_out_date'
        )
    )
    
    # Detect overbookings
    window_overbooking = Window.partitionBy('hotel_id', 'room_number', 'date')
    
    room_flagged = (
        room_daily
        .withColumn('booking_count', count('res_id').over(window_overbooking))
        .withColumn('is_overbooked',
            when(col('booking_count') > 1, lit(True)).otherwise(lit(False)))
    )
    
    # Resolve overbookings (keep first reservation)
    window_resolve = Window.partitionBy('hotel_id', 'room_number', 'date').orderBy('res_id')
    
    return (
        room_flagged
        .withColumn('rn', row_number().over(window_resolve))
        .filter(col('rn') == 1)
        .drop('rn', 'booking_count')
        .select(
            'date', 'hotel_id', 'room_number', 'room_type',
            'is_available', 'res_id', 'guest_id',
            'check_in_date', 'check_out_date', 'is_overbooked'
        )
    )

# COMMAND ----------

# 🥇 GOLD LAYER - Business KPIs

# COMMAND ----------

@dlt.table(
    name="kpi_revpar",
    comment="Revenue Per Available Room - daily by hotel",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "date,hotel_id"
    }
)
def kpi_revpar():
    """Calculate RevPAR: Total Room Revenue / Total Available Rooms"""
    
    # Daily room revenue (explode stays)
    room_revenue_daily = (
        dlt.read("fact_stays_unified")
        .withColumn('date_array',
            expr('sequence(check_in_date, date_sub(check_out_date, 1), interval 1 day)'))
        .withColumn('date', explode(col('date_array')))
        .withColumn('daily_room_revenue',
            col('total_price') / col('stay_length_nights'))
        .groupBy('date', 'hotel_id')
        .agg(spark_sum('daily_room_revenue').alias('total_room_revenue'))
    )
    
    # Total available rooms
    total_rooms_daily = (
        dlt.read("fact_room_availability_daily")
        .groupBy('date', 'hotel_id')
        .agg(countDistinct('room_number').alias('total_available_rooms'))
    )
    
    return (
        total_rooms_daily
        .join(room_revenue_daily, on=['date', 'hotel_id'], how='left')
        .withColumn('total_room_revenue',
            coalesce(col('total_room_revenue'), lit(0.0)))
        .withColumn('revpar',
            when(col('total_available_rooms') > 0,
                 col('total_room_revenue') / col('total_available_rooms'))
            .otherwise(lit(0.0)))
        .select(
            'date', 'hotel_id',
            expr('round(total_room_revenue, 2)').alias('total_room_revenue'),
            'total_available_rooms',
            expr('round(revpar, 2)').alias('revpar')
        )
    )

# COMMAND ----------

@dlt.table(
    name="kpi_adr",
    comment="Average Daily Rate - daily by hotel",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "date,hotel_id"
    }
)
def kpi_adr():
    """Calculate ADR: Total Room Revenue / Rooms Sold"""
    
    return (
        dlt.read("fact_stays_unified")
        .withColumn('date_array',
            expr('sequence(check_in_date, date_sub(check_out_date, 1), interval 1 day)'))
        .withColumn('date', explode(col('date_array')))
        .withColumn('daily_room_revenue',
            col('total_price') / col('stay_length_nights'))
        .groupBy('date', 'hotel_id')
        .agg(
            spark_sum('daily_room_revenue').alias('total_room_revenue'),
            count('*').alias('rooms_sold')
        )
        .withColumn('adr',
            when(col('rooms_sold') > 0,
                 col('total_room_revenue') / col('rooms_sold'))
            .otherwise(lit(0.0)))
        .select(
            'date', 'hotel_id',
            expr('round(total_room_revenue, 2)').alias('total_room_revenue'),
            'rooms_sold',
            expr('round(adr, 2)').alias('adr')
        )
    )

# COMMAND ----------

@dlt.table(
    name="kpi_ancillary_attachment_rate",
    comment="Percentage of guests with ancillary spend",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "date,hotel_id"
    }
)
def kpi_ancillary_attachment_rate():
    """Calculate ancillary attachment rate"""
    
    return (
        dlt.read("fact_stays_unified")
        .withColumn('date_array',
            expr('sequence(check_in_date, date_sub(check_out_date, 1), interval 1 day)'))
        .withColumn('date', explode(col('date_array')))
        .withColumn('has_ancillary_spend',
            when(col('pos_item_count') > 0, lit(1)).otherwise(lit(0)))
        .groupBy('date', 'hotel_id')
        .agg(
            count('*').alias('total_guests'),
            spark_sum('has_ancillary_spend').alias('guests_with_ancillary_spend'),
            spark_sum('total_pos_amount').alias('total_ancillary_revenue')
        )
        .withColumn('attachment_rate_percentage',
            when(col('total_guests') > 0,
                 (col('guests_with_ancillary_spend') / col('total_guests')) * 100)
            .otherwise(lit(0.0)))
        .withColumn('avg_ancillary_per_guest',
            when(col('total_guests') > 0,
                 col('total_ancillary_revenue') / col('total_guests'))
            .otherwise(lit(0.0)))
        .select(
            'date', 'hotel_id', 'total_guests', 'guests_with_ancillary_spend',
            expr('round(attachment_rate_percentage, 2)').alias('attachment_rate_percentage'),
            expr('round(total_ancillary_revenue, 2)').alias('total_ancillary_revenue'),
            expr('round(avg_ancillary_per_guest, 2)').alias('avg_ancillary_per_guest')
        )
    )

# COMMAND ----------

@dlt.table(
    name="kpi_housekeeping_turnover_time",
    comment="Average time between checkout and room cleaned status",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "date,hotel_id"
    }
)
def kpi_housekeeping_turnover_time():
    """Calculate housekeeping turnover time"""
    
    # Get checkout events with room numbers - use already cleaned data
    checkouts = (
        dlt.read("silver_reservations_deduped")
        .filter(col('room_number').isNotNull())
        .filter(col('room_number') != '9999')
        .filter(col('check_out_date').isNotNull())
        .withColumn('checkout_timestamp',
            expr("cast(concat(check_out_date, ' 11:00:00') as timestamp)"))
        .select('res_id', 'hotel_id', 'room_number', 'check_out_date', 'checkout_timestamp')
    )
    
    # Get housekeeping clean status
    housekeeping_clean = (
        dlt.read_stream("bronze_housekeeping_logs")
        .filter(col('log_id').isNotNull())
        .withColumn('status',
            when(col('status').isNotNull(),
                 when(upper(col('status')) == 'CLEAN', 'Clean')
                 .when(upper(col('status')) == 'DIRTY', 'Dirty')
                 .when(upper(col('status')) == 'MAINTENANCE', 'Maintenance')
                 .when(upper(col('status')).contains('ORDER'), 'Out-of-Order')
                 .otherwise('Unknown'))
            .otherwise('Unknown'))
        .withColumn('timestamp',
            coalesce(
                to_timestamp(col('timestamp'), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
                to_timestamp(col('timestamp'), 'dd/MM/yyyy HH:mm'),
                to_timestamp(col('timestamp'), 'yyyy-MM-dd HH:mm:ss')
            ))
        .filter((col('status') == 'Clean') & (col('timestamp').isNotNull()))
        .select('hotel_id', 'room_number', 'status', 'timestamp')
    )
    
    # Join and calculate turnover time
    turnover_calc = (
        checkouts
        .join(housekeeping_clean, on=['hotel_id', 'room_number'], how='inner')
        .filter(
            (col('timestamp') >= col('checkout_timestamp')) &
            (col('timestamp') <= expr("checkout_timestamp + interval 1 day"))
        )
        .withColumn('turnover_time_minutes',
            (unix_timestamp(col('timestamp')) - unix_timestamp(col('checkout_timestamp'))) / 60)
        .filter(
            (col('turnover_time_minutes') > 0) &
            (col('turnover_time_minutes') <= 600)
        )
    )
    
    return (
        turnover_calc
        .groupBy('check_out_date', 'hotel_id')
        .agg(
            count('*').alias('total_checkouts'),
            expr('round(avg(turnover_time_minutes), 2)').alias('avg_turnover_time_minutes'),
            expr('round(percentile_approx(turnover_time_minutes, 0.5), 2)').alias('median_turnover_time_minutes'),
            expr('round(min(turnover_time_minutes), 2)').alias('min_turnover_time_minutes'),
            expr('round(max(turnover_time_minutes), 2)').alias('max_turnover_time_minutes')
        )
        .withColumnRenamed('check_out_date', 'date')
    )

# COMMAND ----------

@dlt.table(
    name="kpi_weekend_vs_weekday_revenue",
    comment="Revenue metrics comparing weekends vs weekdays",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "date,hotel_id"
    }
)
def kpi_weekend_vs_weekday_revenue():
    """Analyze weekend vs weekday revenue patterns"""
    
    # Daily revenue with weekend flag
    daily_revenue = (
        dlt.read("fact_stays_unified")
        .withColumn('date_array',
            expr('sequence(check_in_date, date_sub(check_out_date, 1), interval 1 day)'))
        .withColumn('date', explode(col('date_array')))
        .withColumn('daily_room_revenue',
            col('total_price') / col('stay_length_nights'))
        .withColumn('day_of_week', dayofweek(col('date')))
        .withColumn('is_weekend',
            when(col('day_of_week').isin([1, 7]), lit(True)).otherwise(lit(False)))
    )
    
    # Get total available rooms
    total_rooms = (
        dlt.read("fact_room_availability_daily")
        .groupBy('date', 'hotel_id')
        .agg(countDistinct('room_number').alias('total_rooms'))
    )
    
    return (
        daily_revenue
        .join(total_rooms, on=['date', 'hotel_id'], how='left')
        .groupBy('date', 'hotel_id', 'is_weekend')
        .agg(
            spark_sum('daily_room_revenue').alias('total_room_revenue'),
            count('*').alias('rooms_sold'),
            spark_max('total_rooms').alias('total_available_rooms')
        )
        .withColumn('adr',
            when(col('rooms_sold') > 0,
                 col('total_room_revenue') / col('rooms_sold'))
            .otherwise(lit(0.0)))
        .withColumn('occupancy_rate',
            when(col('total_available_rooms') > 0,
                 (col('rooms_sold') / col('total_available_rooms')) * 100)
            .otherwise(lit(0.0)))
        .withColumn('revpar',
            when(col('total_available_rooms') > 0,
                 col('total_room_revenue') / col('total_available_rooms'))
            .otherwise(lit(0.0)))
        .select(
            'date', 'hotel_id', 'is_weekend',
            expr('round(adr, 2)').alias('adr'),
            expr('round(revpar, 2)').alias('revpar'),
            expr('round(occupancy_rate, 2)').alias('occupancy_rate'),
            'rooms_sold', 'total_available_rooms',
            expr('round(total_room_revenue, 2)').alias('total_room_revenue')
        )
    )

# COMMAND ----------


# 📊 Executive Dashboard View

# COMMAND ----------

@dlt.table(
    name="kpi_executive_dashboard",
    comment="Unified executive dashboard with all key metrics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "date,hotel_id"
    }
)
def kpi_executive_dashboard():
    """Create unified dashboard combining all KPIs"""
    
    return (
        dlt.read("kpi_revpar")
        .select(
            col('date'),
            col('hotel_id'),
            col('revpar')
        )
        .join(
            dlt.read("kpi_adr").select(col('date'), col('hotel_id'), col('adr')),
            on=['date', 'hotel_id'],
            how='left'
        )
        .join(
            dlt.read("kpi_ancillary_attachment_rate").select(
                col('date'),
                col('hotel_id'),
                col('attachment_rate_percentage'),
                col('avg_ancillary_per_guest')
            ),
            on=['date', 'hotel_id'],
            how='left'
        )
    )

# 
# 























































# MAGIC **Shri Radhe Govind Ji! 🙏**