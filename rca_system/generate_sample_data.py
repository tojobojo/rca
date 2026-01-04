
import subprocess
import sys

def install_packages():
    try:
        import openai
        return
    except ImportError:
        pass
    
    packages = [
        "openai>=2.14.0",           # Latest: v2.14.0 (Dec 19, 2025)
        "openai-agents>=0.6.4",     # Latest: v0.6.4 (Dec 2025)
        "pydantic>=2.12.5",         # Latest: v2.12.5 (Nov 26, 2025)
        "litellm>=1.80.11",         # Latest: v1.80.11 (Dec 22, 2025)
        "structlog>=25.5.0",         # Latest: v25.5.0 (Oct 27, 2025)
        "sendgrid>=6.12.5"
    ]
    
    print("Installing required packages...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade"] + packages)
    print("Installation complete!")

# Install packages
install_packages()

# ============================================================================
# Sample Data Generator for RCA System Testing - Merchant Peer Grouping
# ============================================================================
"""
Generates realistic sample data for testing the RCA system.
Creates data for:
- pipeline_step_audit (30 runs with varying drop rates)
- pipeline_rule_definitions (rule versions)
- pipeline_rule_changes (code changes)

Usage:
    python generate_sample_data.py
    
Or in Databricks notebook:
    %run ./generate_sample_data
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import hashlib
import uuid

# ============================================================================
# Schema Definitions
# ============================================================================

class TableSchemas:
    """Schema definitions for all tables"""
    
    @staticmethod
    def get_rule_definitions_schema():
        """Schema for pipeline_rule_definitions table"""
        return StructType([
            StructField("rule_id", StringType(), False),
            StructField("rule_version_id", StringType(), False),
            StructField("pipeline_name", StringType(), False),
            StructField("step_name", StringType(), False),
            StructField("rule_name", StringType(), False),
            StructField("rule_type", StringType(), False),
            StructField("rule_definition", StringType(), False),
            StructField("rule_definition_hash", StringType(), False),
            StructField("version_number", IntegerType(), False),
            StructField("valid_from", TimestampType(), False),
            StructField("valid_to", TimestampType(), True),
            StructField("is_current", BooleanType(), False),
            StructField("change_type", StringType(), False),
            StructField("previous_version_id", StringType(), True),
            StructField("commit_hash", StringType(), False),
            StructField("author", StringType(), False),
            StructField("created_at", TimestampType(), False)
        ])
    
    @staticmethod
    def get_rule_changes_schema():
        """Schema for pipeline_rule_changes table"""
        return StructType([
            StructField("change_id", StringType(), False),
            StructField("change_timestamp", TimestampType(), False),
            StructField("pipeline_name", StringType(), False),
            StructField("step_name", StringType(), False),
            StructField("rule_name", StringType(), False),
            StructField("change_type", StringType(), False),
            StructField("old_rule_version_id", StringType(), False),
            StructField("new_rule_version_id", StringType(), False),
            StructField("old_rule_definition", StringType(), False),
            StructField("new_rule_definition", StringType(), False),
            StructField("diff_summary", StringType(), False),
            StructField("commit_hash", StringType(), False),
            StructField("deployment_timestamp", TimestampType(), False),
            StructField("author", StringType(), False)
        ])
    
    @staticmethod
    def get_step_audit_schema():
        """Schema for pipeline_step_audit table"""
        return StructType([
            StructField("run_id", StringType(), False),
            StructField("pipeline_name", StringType(), False),
            StructField("step_name", StringType(), False),
            StructField("step_order", IntegerType(), False),
            StructField("rule_name", StringType(), False),
            StructField("rule_version", StringType(), False),
            StructField("rule_definition_hash", StringType(), False),
            StructField("input_records", LongType(), False),
            StructField("output_records", LongType(), False),
            StructField("execution_time_seconds", IntegerType(), False),
            StructField("run_timestamp", TimestampType(), False),
            StructField("metadata", MapType(StringType(), StringType()), False)
        ])


# ============================================================================
# Configuration
# ============================================================================

class SampleDataConfig:
    """Configuration for sample data generation"""
    
    # Pipeline configuration
    PIPELINE_NAME = "merchant_peer_grouping_pipeline"
    NUM_RUNS = 30  # Last 30 runs
    
    # Steps and rules configuration
    PIPELINE_STEPS = [
        {
            'name': 'merchant_validation',
            'order': 1,
            'rules': [
                {'name': 'validate_merchant_status', 'base_drop_pct': 4.0, 'variance': 1.5},
                {'name': 'validate_business_category', 'base_drop_pct': 2.5, 'variance': 1.0},
                {'name': 'validate_transaction_volume', 'base_drop_pct': 6.0, 'variance': 2.0},
                {'name': 'validate_account_age', 'base_drop_pct': 3.5, 'variance': 1.5},
            ]
        },
        {
            'name': 'feature_engineering',
            'order': 2,
            'rules': [
                {'name': 'calculate_revenue_metrics', 'base_drop_pct': 2.0, 'variance': 1.0},
                {'name': 'calculate_transaction_patterns', 'base_drop_pct': 3.0, 'variance': 1.5},
                {'name': 'calculate_customer_metrics', 'base_drop_pct': 2.5, 'variance': 1.0},
                {'name': 'calculate_seasonality_features', 'base_drop_pct': 1.5, 'variance': 0.8},
            ]
        },
        {
            'name': 'peer_matching',
            'order': 3,
            'rules': [
                {'name': 'filter_similar_industry', 'base_drop_pct': 8.0, 'variance': 3.0},
                {'name': 'filter_similar_size', 'base_drop_pct': 7.0, 'variance': 2.5},
                {'name': 'filter_geographic_proximity', 'base_drop_pct': 5.0, 'variance': 2.0},
                {'name': 'filter_business_model_match', 'base_drop_pct': 6.5, 'variance': 2.5},
            ]
        },
        {
            'name': 'quality_scoring',
            'order': 4,
            'rules': [
                {'name': 'score_peer_similarity', 'base_drop_pct': 4.0, 'variance': 1.5},
                {'name': 'filter_low_confidence_peers', 'base_drop_pct': 12.0, 'variance': 4.0},
                {'name': 'filter_minimum_peer_count', 'base_drop_pct': 8.0, 'variance': 3.0},
            ]
        },
        {
            'name': 'finalization',
            'order': 5,
            'rules': [
                {'name': 'deduplicate_peer_groups', 'base_drop_pct': 3.0, 'variance': 1.2},
                {'name': 'validate_peer_group_size', 'base_drop_pct': 5.0, 'variance': 2.0},
                {'name': 'filter_incomplete_groups', 'base_drop_pct': 2.0, 'variance': 1.0},
            ]
        }
    ]
    
    # Base input volume
    BASE_INPUT_VOLUME = 500000  # 500K merchants
    
    # Simulate code changes
    CODE_CHANGES = [
        {
            'days_ago': 3,
            'step': 'quality_scoring',
            'rule': 'filter_low_confidence_peers',
            'change_type': 'modified_rule',
            'description': 'Increased confidence threshold from 0.7 to 0.85',
            'impact_multiplier': 1.6  # 60% more drops after this change
        },
        {
            'days_ago': 8,
            'step': 'peer_matching',
            'rule': 'filter_similar_industry',
            'change_type': 'modified_rule',
            'description': 'Changed industry matching from NAICS 4-digit to 6-digit',
            'impact_multiplier': 1.4  # 40% more drops
        },
        {
            'days_ago': 14,
            'step': 'feature_engineering',
            'rule': 'calculate_transaction_patterns',
            'change_type': 'modified_rule',
            'description': 'Added minimum transaction count requirement (100 txns)',
            'impact_multiplier': 1.25  # 25% more drops
        },
        {
            'days_ago': 21,
            'step': 'merchant_validation',
            'rule': 'validate_transaction_volume',
            'change_type': 'modified_rule',
            'description': 'Lowered minimum monthly volume from $1K to $500',
            'impact_multiplier': 0.65  # 35% fewer drops
        },
        {
            'days_ago': 27,
            'step': 'peer_matching',
            'rule': 'filter_geographic_proximity',
            'change_type': 'modified_rule',
            'description': 'Expanded geographic radius from 50 miles to 100 miles',
            'impact_multiplier': 0.75  # 25% fewer drops
        }
    ]


# ============================================================================
# Sample Data Generator
# ============================================================================

class SampleDataGenerator:
    """Generate realistic sample data for testing"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.config = SampleDataConfig()
        self.schemas = TableSchemas()
        
    def generate_all(self):
        """Generate all sample data"""
        print("="*80)
        print("Generating Sample Data for Merchant Peer Grouping RCA System")
        print("="*80)
        
        # Step 1: Generate rule definitions
        print("\n1. Generating rule definitions...")
        rule_definitions = self.generate_rule_definitions()
        self.save_table(
            rule_definitions, 
            "dq_poc.pipeline_rule_definitions",
            self.schemas.get_rule_definitions_schema()
        )
        print(f"   ✓ Created {len(rule_definitions)} rule definitions")
        
        # Step 2: Generate code changes
        print("\n2. Generating code changes...")
        code_changes = self.generate_code_changes()
        self.save_table(
            code_changes, 
            "dq_poc.pipeline_rule_changes",
            self.schemas.get_rule_changes_schema()
        )
        print(f"   ✓ Created {len(code_changes)} code changes")
        
        # Step 3: Generate audit data (30 runs)
        print("\n3. Generating pipeline audit data (30 runs)...")
        audit_records = self.generate_audit_data()
        self.save_table(
            audit_records, 
            "dq_poc.pipeline_step_audit",
            self.schemas.get_step_audit_schema()
        )
        print(f"   ✓ Created {len(audit_records)} audit records")
        
        print("\n" + "="*80)
        print("Sample Data Generation Complete!")
        print("="*80)
        print("\nYou can now test the RCA system with:")
        print("  run_id: Any of the generated run IDs")
        print(f"  pipeline_name: {self.config.PIPELINE_NAME}")
        print("\nExample:")
        print("  orchestrator = PipelineRCAOrchestrator()")
        print(f"  orchestrator.run('run_001', '{self.config.PIPELINE_NAME}', ['merchant-ops@email.com'])")
        print("="*80)
    
    def generate_rule_definitions(self):
        """Generate rule definitions table data"""
        definitions = []
        
        for step in self.config.PIPELINE_STEPS:
            for rule in step['rules']:
                rule_id = f"{self.config.PIPELINE_NAME}:{step['name']}:{rule['name']}"
                rule_version_id = self._generate_version_id()
                
                # Create current version - convert to tuple to match schema
                definitions.append((
                    rule_id,                                              # rule_id
                    rule_version_id,                                      # rule_version_id
                    self.config.PIPELINE_NAME,                            # pipeline_name
                    step['name'],                                         # step_name
                    rule['name'],                                         # rule_name
                    'filter',                                             # rule_type
                    f"-- Rule: {rule['name']}\n-- Base drop rate: {rule['base_drop_pct']}%",  # rule_definition
                    self._generate_hash(rule['name']),                    # rule_definition_hash
                    1,                                                    # version_number
                    datetime.now() - timedelta(days=90),                  # valid_from
                    None,                                                 # valid_to
                    True,                                                 # is_current
                    'new_rule',                                           # change_type
                    None,                                                 # previous_version_id
                    self._generate_commit_hash(),                         # commit_hash
                    'merchant_analytics@company.com',                     # author
                    datetime.now() - timedelta(days=90)                   # created_at
                ))
        
        return definitions
    
    def generate_code_changes(self):
        """Generate code changes table data"""
        changes = []
        
        for change in self.config.CODE_CHANGES:
            change_id = str(uuid.uuid4())
            change_timestamp = datetime.now() - timedelta(days=change['days_ago'])
            
            # Find the rule
            rule_name = change['rule']
            step_name = change['step']
            
            old_version_id = self._generate_version_id()
            new_version_id = self._generate_version_id()
            
            # Convert to tuple to match schema
            changes.append((
                change_id,                                                # change_id
                change_timestamp,                                         # change_timestamp
                self.config.PIPELINE_NAME,                                # pipeline_name
                step_name,                                                # step_name
                rule_name,                                                # rule_name
                change['change_type'],                                    # change_type
                old_version_id,                                           # old_rule_version_id
                new_version_id,                                           # new_rule_version_id
                f"-- Old version of {rule_name}",                         # old_rule_definition
                f"-- New version of {rule_name}\n-- {change['description']}",  # new_rule_definition
                change['description'],                                    # diff_summary
                self._generate_commit_hash(),                             # commit_hash
                change_timestamp,                                         # deployment_timestamp
                random.choice([                                           # author
                    'merchant_analytics@company.com',
                    'data_science@company.com',
                    'peer_grouping_team@company.com'
                ])
            ))
        
        return changes
    
    def generate_audit_data(self):
        """Generate audit table data for 30 runs"""
        audit_records = []
        
        # Create mapping of code changes by day
        code_change_map = {}
        for change in self.config.CODE_CHANGES:
            days_ago = change['days_ago']
            key = (change['step'], change['rule'])
            code_change_map[key] = {
                'days_ago': days_ago,
                'multiplier': change['impact_multiplier']
            }
        
        # Generate 30 runs (one per day for last 30 days)
        for run_num in range(self.config.NUM_RUNS):
            days_ago = self.config.NUM_RUNS - run_num - 1
            run_id = f"run_{run_num:03d}"
            run_timestamp = datetime.now() - timedelta(days=days_ago, hours=10, minutes=30)
            
            # Start with base input volume (with some daily variation)
            current_volume = int(self.config.BASE_INPUT_VOLUME * random.uniform(0.95, 1.05))
            
            # Process each step
            for step in self.config.PIPELINE_STEPS:
                step_input = current_volume
                
                # Process each rule in the step
                for rule in step['rules']:
                    rule_input = current_volume
                    
                    # Calculate drop percentage
                    base_drop_pct = rule['base_drop_pct']
                    variance = rule['variance']
                    
                    # Add random variance
                    drop_pct = base_drop_pct + random.uniform(-variance, variance)
                    
                    # Apply code change impact if applicable
                    change_key = (step['name'], rule['name'])
                    if change_key in code_change_map:
                        change_info = code_change_map[change_key]
                        # Apply multiplier if this run is after the code change
                        if days_ago <= change_info['days_ago']:
                            drop_pct *= change_info['multiplier']
                    
                    # Add some anomalies randomly (5% chance)
                    if random.random() < 0.05:
                        drop_pct *= random.uniform(1.5, 2.5)  # Spike
                    
                    # Ensure drop percentage is between 0 and 100
                    drop_pct = max(0, min(100, drop_pct))
                    
                    # Calculate counts
                    dropped = int(rule_input * (drop_pct / 100))
                    rule_output = rule_input - dropped
                    
                    # Create audit record - convert to tuple to match schema
                    audit_records.append((
                        run_id,                                           # run_id
                        self.config.PIPELINE_NAME,                        # pipeline_name
                        step['name'],                                     # step_name
                        step['order'],                                    # step_order
                        rule['name'],                                     # rule_name
                        self._generate_version_id(),                      # rule_version
                        self._generate_hash(rule['name']),                # rule_definition_hash
                        rule_input,                                       # input_records
                        rule_output,                                      # output_records
                        random.randint(10, 120),                          # execution_time_seconds
                        run_timestamp,                                    # run_timestamp
                        {}                                                # metadata (empty dict)
                    ))
                    
                    # Update current volume for next rule
                    current_volume = rule_output
        
        return audit_records
    
    def save_table(self, data, table_name, schema):
        """Save data to Delta table with explicit schema"""
        df = self.spark.createDataFrame(data, schema)
        
        # Create table if it doesn't exist
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)
    
    def _generate_version_id(self):
        """Generate a version ID"""
        return f"v_{uuid.uuid4().hex[:12]}"
    
    def _generate_hash(self, text):
        """Generate a hash"""
        return hashlib.sha256(text.encode()).hexdigest()[:16]
    
    def _generate_commit_hash(self):
        """Generate a commit hash"""
        return hashlib.sha256(str(uuid.uuid4()).encode()).hexdigest()[:8]


# ============================================================================
# Display Sample Data
# ============================================================================

class SampleDataViewer:
    """View generated sample data"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def show_overview(self):
        """Show overview of generated data"""
        print("\n" + "="*80)
        print("Sample Data Overview - Merchant Peer Grouping Pipeline")
        print("="*80)
        
        # Audit data
        print("\n1. Pipeline Step Audit")
        print("-"*80)
        audit_df = self.spark.table("dq_poc.pipeline_step_audit")
        print(f"   Total records: {audit_df.count()}")
        print(f"   Unique runs: {audit_df.select('run_id').distinct().count()}")
        print(f"   Date range: {audit_df.agg({'run_timestamp': 'min'}).collect()[0][0]} to {audit_df.agg({'run_timestamp': 'max'}).collect()[0][0]}")
        
        print("\n   Sample records:")
        audit_df.select('run_id', 'step_name', 'rule_name', 'input_records', 'output_records', 'run_timestamp') \
            .orderBy('run_timestamp', ascending=False) \
            .show(5, truncate=False)
        
        # Rule definitions
        print("\n2. Pipeline Rule Definitions")
        print("-"*80)
        rules_df = self.spark.table("dq_poc.pipeline_rule_definitions")
        print(f"   Total rules: {rules_df.count()}")
        
        print("\n   Rules by step:")
        rules_df.groupBy('step_name').count().orderBy('step_name').show(truncate=False)
        
        # Code changes
        print("\n3. Pipeline Rule Changes")
        print("-"*80)
        changes_df = self.spark.table("dq_poc.pipeline_rule_changes")
        print(f"   Total changes: {changes_df.count()}")
        
        print("\n   Recent changes:")
        changes_df.select('change_timestamp', 'step_name', 'rule_name', 'change_type', 'diff_summary') \
            .orderBy('change_timestamp', ascending=False) \
            .show(truncate=False)
        
        print("\n" + "="*80)
    
    def show_drop_trends(self):
        """Show drop rate trends"""
        print("\n" + "="*80)
        print("Drop Rate Trends Analysis - Merchant Peer Grouping")
        print("="*80)
        
        audit_df = self.spark.table("dq_poc.pipeline_step_audit")
        
        # Calculate drop percentage and show trends
        from pyspark.sql import functions as F
        
        trends_df = audit_df.withColumn(
            'drop_percentage',
            ((F.col('input_records') - F.col('output_records')) / F.col('input_records') * 100)
        )
        
        print("\n1. Average Drop Rate by Step:")
        trends_df.groupBy('step_name') \
            .agg(
                F.avg('drop_percentage').alias('avg_drop_pct'),
                F.max('drop_percentage').alias('max_drop_pct'),
                F.min('drop_percentage').alias('min_drop_pct')
            ) \
            .orderBy('step_name') \
            .show(truncate=False)
        
        print("\n2. Average Drop Rate by Rule (Top 10):")
        trends_df.groupBy('rule_name') \
            .agg(
                F.avg('drop_percentage').alias('avg_drop_pct'),
                F.max('drop_percentage').alias('max_drop_pct'),
                F.count('*').alias('run_count')
            ) \
            .orderBy(F.desc('avg_drop_pct')) \
            .show(10, truncate=False)
        
        print("\n3. Drop Rate Over Time (Last 10 runs):")
        trends_df.select(
            'run_id',
            F.date_format('run_timestamp', 'yyyy-MM-dd').alias('date'),
            'step_name',
            'rule_name',
            F.round('drop_percentage', 2).alias('drop_pct')
        ).orderBy('run_timestamp', ascending=False).show(10, truncate=False)
        
        print("="*80)


# ============================================================================
# Quick Test Function
# ============================================================================

def quick_test_rca():
    """Quick test of RCA system with generated data"""
    print("\n" + "="*80)
    print("Quick RCA Test - Merchant Peer Grouping")
    print("="*80)
    
    from main import PipelineRCAOrchestrator
    
    # Test with most recent run
    spark = SparkSession.builder.getOrCreate()
    latest_run = spark.sql("""
        SELECT run_id, pipeline_name
        FROM dq_poc.pipeline_step_audit
        ORDER BY run_timestamp DESC
        LIMIT 1
    """).first()
    
    if not latest_run:
        print("❌ No data found. Run generate_all() first.")
        return
    
    print(f"\nTesting with run_id: {latest_run.run_id}")
    print(f"Pipeline: {latest_run.pipeline_name}")
    
    try:
        orchestrator = PipelineRCAOrchestrator()
        result = orchestrator.run(
            run_id=latest_run.run_id,
            pipeline_name=latest_run.pipeline_name
        )
        
        if result['success']:
            print("\n✓ RCA Test Successful!")
            print(f"  Severity: {result['severity']}")
            print(f"  Merchants processed: {result['metrics_records']}")
            print(f"\n  Summary preview:")
            print("  " + result['summary'][:300] + "...")
        else:
            print(f"\n❌ RCA Test Failed: {result['error']}")
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    print("="*80)


# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution function"""
    spark = SparkSession.builder \
        .appName("Merchant Peer Grouping RCA Sample Data Generator") \
        .getOrCreate()
    
    print("\n" + "="*80)
    print("RCA System - Merchant Peer Grouping Sample Data Generator")
    print("="*80)
    print("\nWhat would you like to do?")
    print("  1. Generate all sample data")
    print("  2. View existing data")
    print("  3. Show drop trends")
    print("  4. Quick RCA test")
    print("  5. Generate data + Run test")
    
    choice = input("\nEnter choice (1-5): ").strip()
    
    if choice == '1':
        generator = SampleDataGenerator(spark)
        generator.generate_all()
    
    elif choice == '2':
        viewer = SampleDataViewer(spark)
        viewer.show_overview()
    
    elif choice == '3':
        viewer = SampleDataViewer(spark)
        viewer.show_drop_trends()
    
    elif choice == '4':
        quick_test_rca()
    
    elif choice == '5':
        generator = SampleDataGenerator(spark)
        generator.generate_all()
        
        viewer = SampleDataViewer(spark)
        viewer.show_overview()
        
        quick_test_rca()
    
    else:
        print("Invalid choice")


# ============================================================================
# For Databricks Notebook Usage
# ============================================================================

def generate_sample_data_for_databricks():
    """
    Simple function for Databricks notebook usage
    Just run this cell!
    """
    spark = SparkSession.builder.getOrCreate()
    
    # Generate data
    generator = SampleDataGenerator(spark)
    generator.generate_all()
    
    # Show overview
    viewer = SampleDataViewer(spark)
    viewer.show_overview()
    viewer.show_drop_trends()
    
    return "Merchant peer grouping sample data generated successfully!"


if __name__ == "__main__":
    # For command line execution
    main()
else:
    # For Databricks notebook, auto-run
    print("Merchant Peer Grouping sample data generator loaded.")
    print("Run: generate_sample_data_for_databricks()")
    print("Or: spark = SparkSession.builder.getOrCreate()")
    print("    generator = SampleDataGenerator(spark)")
    print("    generator.generate_all()")