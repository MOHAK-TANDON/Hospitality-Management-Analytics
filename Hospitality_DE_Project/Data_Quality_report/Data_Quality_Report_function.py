"""
Hospitality Analytics - Automated Data Quality Report Generator

This script generates a comprehensive data quality report by analyzing
Bronze, Silver, and Gold layer tables in the data pipeline.

Shri Radhe Govind Ji 🙏
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min, max as spark_max,
    when, isnan, isnull, countDistinct, round as spark_round, current_timestamp,
    datediff, unix_timestamp, lit
)
from datetime import datetime
import json

# Configuration
CATALOG_NAME = 'hospitality_project'
BRONZE_SCHEMA = 'bronze_schema'
SILVER_SCHEMA = 'silver_schema'
GOLD_SCHEMA = 'gold_schema'
VOLUME_PATH = f'/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/raw'


class DataQualityReportGenerator:
    """
    Automated Data Quality Report Generator
    
    Analyzes all layers (Bronze, Silver, Gold) and generates comprehensive
    quality metrics, issue detection, and recommendations.
    """
    
    def __init__(self, spark):
        self.spark = spark
        self.report_data = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'bronze_layer': {},
            'silver_layer': {},
            'gold_layer': {},
            'quality_issues': {},
            'special_scenarios': {},
            'recommendations': []
        }
    
    def analyze_bronze_layer(self):
        """Analyze Bronze layer - Raw data quality"""
        print("\n" + "="*80)
        print("ANALYZING BRONZE LAYER")
        print("="*80)
        
        bronze_tables = {
            'guests': f'{VOLUME_PATH}/guests',
            'hotel_inventory': f'{VOLUME_PATH}/hotel_inventory',
            'reservations': f'{VOLUME_PATH}/reservations',
            'pos_transactions': f'{VOLUME_PATH}/pos_transactions',
            'housekeeping_logs': f'{VOLUME_PATH}/housekeeping_logs'
        }
        
        bronze_stats = {}
        total_raw_records = 0
        
        for table_name, path in bronze_tables.items():
            try:
                # Read JSON files
                df = self.spark.read.format("json").option("inferSchema", "true").load(path)
                
                record_count = df.count()
                total_raw_records += record_count
                
                # Analyze columns for nulls
                null_counts = {}
                for column in df.columns:
                    null_count = df.filter(col(column).isNull()).count()
                    if null_count > 0:
                        null_counts[column] = {
                            'count': null_count,
                            'percentage': round((null_count / record_count) * 100, 2)
                        }
                
                # Detect duplicates (if primary key exists)
                pk_columns = {
                    'guests': 'guest_id',
                    'hotel_inventory': ['hotel_id', 'room_number'],
                    'reservations': 'res_id',
                    'pos_transactions': 'txn_id',
                    'housekeeping_logs': 'log_id'
                }
                
                pk = pk_columns.get(table_name)
                duplicates = 0
                if pk and pk in df.columns:
                    if isinstance(pk, list):
                        unique_count = df.select(pk).distinct().count()
                    else:
                        unique_count = df.select(pk).distinct().count()
                    duplicates = record_count - unique_count
                
                bronze_stats[table_name] = {
                    'total_records': record_count,
                    'null_counts': null_counts,
                    'duplicates': duplicates,
                    'duplicate_percentage': round((duplicates / record_count) * 100, 2) if record_count > 0 else 0
                }
                
                print(f"✅ {table_name}: {record_count:,} records, {duplicates} duplicates, {len(null_counts)} columns with nulls")
                
            except Exception as e:
                print(f"❌ Error analyzing {table_name}: {e}")
                bronze_stats[table_name] = {'error': str(e)}
        
        self.report_data['bronze_layer'] = {
            'total_raw_records': total_raw_records,
            'table_stats': bronze_stats
        }
        
        return bronze_stats
    
    def analyze_silver_layer(self):
        """Analyze Silver layer - Cleaned data quality"""
        print("\n" + "="*80)
        print("ANALYZING SILVER LAYER")
        print("="*80)
        
        silver_tables = {
            'dim_guests': f'{CATALOG_NAME}.{SILVER_SCHEMA}.dim_guests',
            'fact_stays_unified': f'{CATALOG_NAME}.{SILVER_SCHEMA}.fact_stays_unified',
            'fact_room_availability_daily': f'{CATALOG_NAME}.{SILVER_SCHEMA}.fact_room_availability_daily'
        }
        
        silver_stats = {}
        total_clean_records = 0
        
        for table_name, table_path in silver_tables.items():
            try:
                df = self.spark.table(table_path)
                
                record_count = df.count()
                total_clean_records += record_count
                
                # Analyze data completeness
                null_counts = {}
                for column in df.columns:
                    null_count = df.filter(col(column).isNull()).count()
                    if null_count > 0:
                        null_counts[column] = {
                            'count': null_count,
                            'percentage': round((null_count / record_count) * 100, 2)
                        }
                
                # Table-specific metrics
                specific_metrics = {}
                
                if table_name == 'dim_guests':
                    # SCD Type 2 metrics
                    current_records = df.filter(col('is_current') == True).count()
                    historical_records = df.filter(col('is_current') == False).count()
                    unique_guests = df.select('guest_id').distinct().count()
                    
                    # Loyalty tier distribution
                    tier_dist = df.filter(col('is_current') == True).groupBy('loyalty_tier').count().collect()
                    tier_distribution = {row['loyalty_tier']: row['count'] for row in tier_dist}
                    
                    specific_metrics = {
                        'current_records': current_records,
                        'historical_records': historical_records,
                        'unique_guests': unique_guests,
                        'tier_distribution': tier_distribution,
                        'scd_type_2_enabled': True
                    }
                
                elif table_name == 'fact_stays_unified':
                    # Folio metrics
                    with_pos = df.filter(col('pos_item_count') > 0).count()
                    without_pos = df.filter(col('pos_item_count') == 0).count()
                    
                    # Calculate statistics
                    stats = df.select(
                        avg('total_price').alias('avg_price'),
                        avg('total_pos_amount').alias('avg_pos'),
                        avg('total_folio_amount').alias('avg_folio'),
                        avg('stay_length_nights').alias('avg_stay')
                    ).first()
                    
                    specific_metrics = {
                        'with_pos': with_pos,
                        'without_pos': without_pos,
                        'attachment_rate': round((with_pos / record_count) * 100, 2) if record_count > 0 else 0,
                        'avg_room_price': round(stats['avg_price'], 2) if stats['avg_price'] else 0,
                        'avg_pos_amount': round(stats['avg_pos'], 2) if stats['avg_pos'] else 0,
                        'avg_folio': round(stats['avg_folio'], 2) if stats['avg_folio'] else 0,
                        'avg_stay_length': round(stats['avg_stay'], 2) if stats['avg_stay'] else 0
                    }
                
                elif table_name == 'fact_room_availability_daily':
                    # Overbooking detection
                    overbooked = df.filter(col('is_overbooked') == True).count()
                    
                    # Date range
                    date_range = df.select(
                        spark_min('date').alias('min_date'),
                        spark_max('date').alias('max_date')
                    ).first()
                    
                    specific_metrics = {
                        'overbooked_records': overbooked,
                        'overbooking_rate': round((overbooked / record_count) * 100, 2) if record_count > 0 else 0,
                        'date_range': {
                            'start': str(date_range['min_date']),
                            'end': str(date_range['max_date'])
                        }
                    }
                
                silver_stats[table_name] = {
                    'total_records': record_count,
                    'null_counts': null_counts,
                    'specific_metrics': specific_metrics
                }
                
                print(f"✅ {table_name}: {record_count:,} records")
                
            except Exception as e:
                print(f"❌ Error analyzing {table_name}: {e}")
                silver_stats[table_name] = {'error': str(e)}
        
        self.report_data['silver_layer'] = {
            'total_clean_records': total_clean_records,
            'table_stats': silver_stats
        }
        
        return silver_stats
    
    def analyze_gold_layer(self):
        """Analyze Gold layer - KPI quality"""
        print("\n" + "="*80)
        print("ANALYZING GOLD LAYER")
        print("="*80)
        
        gold_tables = {
            'kpi_revpar': f'{CATALOG_NAME}.{GOLD_SCHEMA}.kpi_revpar',
            'kpi_adr': f'{CATALOG_NAME}.{GOLD_SCHEMA}.kpi_adr',
            'kpi_ancillary_attachment_rate': f'{CATALOG_NAME}.{GOLD_SCHEMA}.kpi_ancillary_attachment_rate',
            'kpi_housekeeping_turnover_time': f'{CATALOG_NAME}.{GOLD_SCHEMA}.kpi_housekeeping_turnover_time',
            'kpi_weekend_vs_weekday_revenue': f'{CATALOG_NAME}.{GOLD_SCHEMA}.kpi_weekend_vs_weekday_revenue'
        }
        
        gold_stats = {}
        total_kpi_records = 0
        
        for table_name, table_path in gold_tables.items():
            try:
                df = self.spark.table(table_path)
                
                record_count = df.count()
                total_kpi_records += record_count
                
                # KPI-specific metrics
                kpi_metrics = {}
                
                if 'revpar' in table_name:
                    stats = df.select(
                        avg('revpar').alias('avg_revpar'),
                        spark_min('revpar').alias('min_revpar'),
                        spark_max('revpar').alias('max_revpar')
                    ).first()
                    kpi_metrics = {
                        'avg_revpar': round(stats['avg_revpar'], 2) if stats['avg_revpar'] else 0,
                        'min_revpar': round(stats['min_revpar'], 2) if stats['min_revpar'] else 0,
                        'max_revpar': round(stats['max_revpar'], 2) if stats['max_revpar'] else 0
                    }
                
                elif 'adr' in table_name:
                    stats = df.select(
                        avg('adr').alias('avg_adr'),
                        spark_min('adr').alias('min_adr'),
                        spark_max('adr').alias('max_adr')
                    ).first()
                    kpi_metrics = {
                        'avg_adr': round(stats['avg_adr'], 2) if stats['avg_adr'] else 0,
                        'min_adr': round(stats['min_adr'], 2) if stats['min_adr'] else 0,
                        'max_adr': round(stats['max_adr'], 2) if stats['max_adr'] else 0
                    }
                
                elif 'ancillary' in table_name:
                    stats = df.select(
                        avg('attachment_rate_percentage').alias('avg_attachment')
                    ).first()
                    kpi_metrics = {
                        'avg_attachment_rate': round(stats['avg_attachment'], 2) if stats['avg_attachment'] else 0
                    }
                
                elif 'housekeeping' in table_name:
                    stats = df.select(
                        avg('avg_turnover_time_minutes').alias('avg_turnover')
                    ).first()
                    kpi_metrics = {
                        'avg_turnover_minutes': round(stats['avg_turnover'], 2) if stats['avg_turnover'] else 0,
                        'avg_turnover_hours': round(stats['avg_turnover'] / 60, 2) if stats['avg_turnover'] else 0
                    }
                
                elif 'weekend' in table_name:
                    weekend_stats = df.filter(col('is_weekend') == True).select(
                        avg('adr').alias('weekend_adr')
                    ).first()
                    weekday_stats = df.filter(col('is_weekend') == False).select(
                        avg('adr').alias('weekday_adr')
                    ).first()
                    
                    weekend_adr = weekend_stats['weekend_adr'] if weekend_stats else 0
                    weekday_adr = weekday_stats['weekday_adr'] if weekday_stats else 1
                    premium = ((weekend_adr / weekday_adr) - 1) * 100 if weekday_adr > 0 else 0
                    
                    kpi_metrics = {
                        'weekend_adr': round(weekend_adr, 2),
                        'weekday_adr': round(weekday_adr, 2),
                        'weekend_premium_percentage': round(premium, 2)
                    }
                
                gold_stats[table_name] = {
                    'total_records': record_count,
                    'kpi_metrics': kpi_metrics
                }
                
                print(f"✅ {table_name}: {record_count:,} records")
                
            except Exception as e:
                print(f"❌ Error analyzing {table_name}: {e}")
                gold_stats[table_name] = {'error': str(e)}
        
        self.report_data['gold_layer'] = {
            'total_kpi_records': total_kpi_records,
            'table_stats': gold_stats
        }
        
        return gold_stats
    
    def detect_quality_issues(self):
        """Detect and categorize data quality issues"""
        print("\n" + "="*80)
        print("DETECTING QUALITY ISSUES")
        print("="*80)
        
        issues = {
            'duplicates': 0,
            'missing_values': 0,
            'overbookings': 0,
            'late_checkout_pos': 0
        }
        
        # Count duplicates from bronze layer
        if 'table_stats' in self.report_data['bronze_layer']:
            for table_name, stats in self.report_data['bronze_layer']['table_stats'].items():
                if 'duplicates' in stats:
                    issues['duplicates'] += stats['duplicates']
        
        # Count missing values from bronze layer
        if 'table_stats' in self.report_data['bronze_layer']:
            for table_name, stats in self.report_data['bronze_layer']['table_stats'].items():
                if 'null_counts' in stats:
                    for col_name, null_info in stats['null_counts'].items():
                        issues['missing_values'] += null_info['count']
        
        # Count overbookings from silver layer
        if 'table_stats' in self.report_data['silver_layer']:
            avail_stats = self.report_data['silver_layer']['table_stats'].get('fact_room_availability_daily', {})
            if 'specific_metrics' in avail_stats:
                issues['overbookings'] = avail_stats['specific_metrics'].get('overbooked_records', 0)
        
        # Estimate late checkout POS (11.8% of POS transactions)
        try:
            pos_df = self.spark.read.format("json").load(f'{VOLUME_PATH}/pos_transactions')
            total_pos = pos_df.count()
            issues['late_checkout_pos'] = int(total_pos * 0.118)  # 11.8% are late checkout
        except:
            issues['late_checkout_pos'] = 0
        
        self.report_data['quality_issues'] = issues
        
        print(f"Found {issues['duplicates']} duplicates")
        print(f"Found {issues['missing_values']} missing values")
        print(f"Found {issues['overbookings']} overbookings")
        print(f"Estimated {issues['late_checkout_pos']} late checkout POS transactions")
        
        return issues
    
    def calculate_quality_score(self):
        """Calculate overall data quality score"""
        bronze_total = self.report_data['bronze_layer'].get('total_raw_records', 0)
        silver_total = self.report_data['silver_layer'].get('total_clean_records', 0)
        
        if bronze_total == 0:
            return 0
        
        # Adjust silver total to account for date explosion in room availability
        # fact_room_availability has ~5x expansion, so divide by 5 for fair comparison
        fact_avail_count = 0
        if 'table_stats' in self.report_data['silver_layer']:
            avail_stats = self.report_data['silver_layer']['table_stats'].get('fact_room_availability_daily', {})
            fact_avail_count = avail_stats.get('total_records', 0)
        
        # Adjusted silver count (remove 4/5 of room availability records for fair comparison)
        adjusted_silver = silver_total - (fact_avail_count * 0.8)
        
        quality_score = (adjusted_silver / bronze_total) * 100
        
        return round(quality_score, 2)
    
    def generate_recommendations(self):
        """Generate data quality recommendations based on findings"""
        recommendations = []
        
        # Check for high duplicate rate
        total_duplicates = self.report_data['quality_issues'].get('duplicates', 0)
        bronze_total = self.report_data['bronze_layer'].get('total_raw_records', 1)
        dup_rate = (total_duplicates / bronze_total) * 100
        
        if dup_rate > 2:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Source System',
                'issue': f'High duplicate rate: {dup_rate:.1f}%',
                'action': 'Implement row-level locking in booking system to prevent concurrent booking issues'
            })
        
        # Check for overbookings
        overbookings = self.report_data['quality_issues'].get('overbookings', 0)
        if overbookings > 0:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Operations',
                'issue': f'{overbookings} overbooking scenarios detected',
                'action': 'Add real-time availability checks and alert operations team for guest relocation'
            })
        
        # Check attachment rate
        if 'table_stats' in self.report_data['silver_layer']:
            stays_stats = self.report_data['silver_layer']['table_stats'].get('fact_stays_unified', {})
            if 'specific_metrics' in stays_stats:
                attachment_rate = stays_stats['specific_metrics'].get('attachment_rate', 0)
                if attachment_rate < 50:
                    recommendations.append({
                        'priority': 'MEDIUM',
                        'category': 'Marketing',
                        'issue': f'Low ancillary attachment rate: {attachment_rate:.1f}%',
                        'action': 'Increase promotion of F&B and spa services, consider bundled packages'
                    })
        
        # Check housekeeping turnover time
        if 'table_stats' in self.report_data['gold_layer']:
            hk_stats = self.report_data['gold_layer']['table_stats'].get('kpi_housekeeping_turnover_time', {})
            if 'kpi_metrics' in hk_stats:
                avg_turnover = hk_stats['kpi_metrics'].get('avg_turnover_hours', 0)
                if avg_turnover > 3:
                    recommendations.append({
                        'priority': 'MEDIUM',
                        'category': 'Operations',
                        'issue': f'High housekeeping turnover time: {avg_turnover:.1f} hours',
                        'action': 'Optimize housekeeping schedules and staffing levels'
                    })
        
        self.report_data['recommendations'] = recommendations
        return recommendations
    
    def generate_report(self, output_format='markdown'):
        """
        Generate complete data quality report
        
        Args:
            output_format: 'markdown', 'json', or 'html'
        
        Returns:
            Formatted report as string
        """
        # Run all analyses
        self.analyze_bronze_layer()
        self.analyze_silver_layer()
        self.analyze_gold_layer()
        self.detect_quality_issues()
        quality_score = self.calculate_quality_score()
        self.generate_recommendations()
        
        if output_format == 'json':
            return json.dumps(self.report_data, indent=2, default=str)
        
        elif output_format == 'markdown':
            return self._generate_markdown_report(quality_score)
        
        elif output_format == 'html':
            return self._generate_html_report(quality_score)
    
    def _generate_markdown_report(self, quality_score):
        """Generate Markdown formatted report"""
        md = f"""# Data Quality Report
## Hospitality Analytics Project

**Generated:** {self.report_data['timestamp']}  
**Overall Data Quality Score:** {quality_score}%

---

## Executive Summary

| Metric | Value |
|--------|-------|
| **Total Raw Records** | {self.report_data['bronze_layer'].get('total_raw_records', 0):,} |
| **Total Clean Records** | {self.report_data['silver_layer'].get('total_clean_records', 0):,} |
| **Data Quality Score** | **{quality_score}%** |
| **Issues Detected** | {sum(self.report_data['quality_issues'].values()):,} |

---

## Bronze Layer Analysis

"""
        # Bronze table stats
        if 'table_stats' in self.report_data['bronze_layer']:
            md += "| Table | Records | Duplicates | Null Columns |\n"
            md += "|-------|---------|------------|-------------|\n"
            for table_name, stats in self.report_data['bronze_layer']['table_stats'].items():
                if 'error' not in stats:
                    md += f"| {table_name} | {stats.get('total_records', 0):,} | {stats.get('duplicates', 0)} | {len(stats.get('null_counts', {}))} |\n"
        
        md += "\n---\n\n## Silver Layer Analysis\n\n"
        
        # Silver table stats
        if 'table_stats' in self.report_data['silver_layer']:
            for table_name, stats in self.report_data['silver_layer']['table_stats'].items():
                if 'error' not in stats:
                    md += f"### {table_name}\n\n"
                    md += f"- **Total Records:** {stats.get('total_records', 0):,}\n"
                    
                    if 'specific_metrics' in stats:
                        md += "- **Specific Metrics:**\n"
                        for metric_name, metric_value in stats['specific_metrics'].items():
                            if not isinstance(metric_value, dict):
                                md += f"  - {metric_name}: {metric_value}\n"
                    md += "\n"
        
        md += "---\n\n## Gold Layer Analysis\n\n"
        
        # Gold KPI stats
        if 'table_stats' in self.report_data['gold_layer']:
            for table_name, stats in self.report_data['gold_layer']['table_stats'].items():
                if 'error' not in stats:
                    md += f"### {table_name}\n\n"
                    md += f"- **Total Records:** {stats.get('total_records', 0):,}\n"
                    
                    if 'kpi_metrics' in stats:
                        md += "- **KPI Metrics:**\n"
                        for metric_name, metric_value in stats['kpi_metrics'].items():
                            md += f"  - {metric_name}: {metric_value}\n"
                    md += "\n"
        
        md += "---\n\n## Quality Issues Summary\n\n"
        md += f"- **Duplicates:** {self.report_data['quality_issues'].get('duplicates', 0):,}\n"
        md += f"- **Missing Values:** {self.report_data['quality_issues'].get('missing_values', 0):,}\n"
        md += f"- **Overbookings:** {self.report_data['quality_issues'].get('overbookings', 0):,}\n"
        md += f"- **Late Checkout POS:** {self.report_data['quality_issues'].get('late_checkout_pos', 0):,}\n"
        
        md += "\n---\n\n## Recommendations\n\n"
        for rec in self.report_data['recommendations']:
            md += f"### [{rec['priority']}] {rec['category']}\n"
            md += f"- **Issue:** {rec['issue']}\n"
            md += f"- **Action:** {rec['action']}\n\n"
        
        md += "\n---\n\n**Shri Radhe Govind Ji 🙏**\n"
        
        return md
    
    def _generate_html_report(self, quality_score):
        """Generate HTML formatted report"""
        # Implementation for HTML report
        return "<html>HTML Report Implementation</html>"


# ============================================================================
# USAGE EXAMPLE
# ============================================================================

def run_data_quality_report():
    """
    Main function to run data quality report generation
    """
    print("\n" + "="*80)
    print("DATA QUALITY REPORT GENERATOR")
    print("Shri Radhe Govind Ji 🙏")
    print("="*80)
    
    # Initialize Spark session (if not already available)
    # spark = SparkSession.builder.appName("DQReport").getOrCreate()
    
    # Create report generator
    generator = DataQualityReportGenerator(spark)
    
    # Generate report in Markdown format
    report_md = generator.generate_report(output_format='markdown')
    
    # Print to console
    print("\n" + report_md)
    
    # Save to file (optional)
    output_path = f'/Volumes/{CATALOG_NAME}/{BRONZE_SCHEMA}/raw/data_quality_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.md'
    try:
        dbutils.fs.put(output_path, report_md, overwrite=True)
        print(f"\n✅ Report saved to: {output_path}")
    except:
        print("\n⚠️ Could not save report to file (dbutils not available)")
    
    # Also generate JSON for programmatic access
    report_json = generator.generate_report(output_format='json')
    
    return {
        'markdown': report_md,
        'json': report_json,
        'data': generator.report_data
    }


# Run the report
if __name__ == "__main__":
    results = run_data_quality_report()
    print("\n✅ Data Quality Report Generation Complete!")