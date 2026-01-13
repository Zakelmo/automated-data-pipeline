import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)

class S3Simulator:
    def __init__(self):
        self.storage = {}
        self.buckets = {}
        self.base_path = Path("data/raw")
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.info("S3 Simulator initialized")

    def create_bucket(self, bucket_name):
        self.buckets[bucket_name] = {
            'created': datetime.now(),
            'objects': []
        }
        logger.info(f"Bucket created: {bucket_name}")

    def upload_data(self, df, key):
        self.storage[key] = df
        csv_path = self.base_path / key.replace('/', '_')
        df.to_csv(csv_path, index=False)
        logger.info(f"Data uploaded: {key} ({len(df)} records)")
        return {'key': key, 'size': len(df)}

    def download_data(self, key):
        return self.storage.get(key)

    def list_objects(self, prefix=""):
        return [k for k in self.storage.keys() if k.startswith(prefix)]

class GlueSimulator:
    def __init__(self):
        self.jobs = []
        self.crawlers = []
        logger.info("Glue Simulator initialized")

    def process_data(self, df):
        logger.info(f"Glue ETL processing {len(df)} records")

        processed_df = df.copy()

        # Remove duplicates
        initial_rows = len(processed_df)
        processed_df = processed_df.drop_duplicates()

        # Handle missing values
        for col in processed_df.columns:
            if processed_df[col].dtype in ['float64', 'int64']:
                processed_df[col].fillna(processed_df[col].median(), inplace=True)
            else:
                processed_df[col].fillna('Unknown', inplace=True)

        # Add metadata
        processed_df['processed_timestamp'] = datetime.now().isoformat()
        processed_df['data_quality'] = np.random.uniform(0.9, 1.0, len(processed_df))

        logger.info(f"Processed: {len(processed_df)} records, removed {initial_rows - len(processed_df)} duplicates")
        return processed_df

    def create_crawler(self, name, database, s3_target):
        crawler = {
            'name': name,
            'database': database,
            's3_target': s3_target,
            'created': datetime.now()
        }
        self.crawlers.append(crawler)
        logger.info(f"Crawler created: {name}")
        return crawler

class LambdaSimulator:
    def __init__(self):
        self.functions = {}
        self.invocations = []
        logger.info("Lambda Simulator initialized")

    def transform_data(self, df):
        logger.info(f"Lambda transformation for {len(df)} records")

        transformed_df = df.copy()

        # Add calculated fields
        numeric_cols = transformed_df.select_dtypes(include=['number']).columns.tolist()

        if len(numeric_cols) > 0:
            # Calculate aggregations
            transformed_df['numeric_sum'] = transformed_df[numeric_cols].sum(axis=1)
            transformed_df['numeric_mean'] = transformed_df[numeric_cols].mean(axis=1)
            transformed_df['numeric_std'] = transformed_df[numeric_cols].std(axis=1)

            # Add normalized columns
            for col in numeric_cols[:min(3, len(numeric_cols))]:
                col_min = transformed_df[col].min()
                col_max = transformed_df[col].max()
                if col_max > col_min:
                    transformed_df[f'{col}_normalized'] = (transformed_df[col] - col_min) / (col_max - col_min)

        # Add transformation metadata
        transformed_df['transform_timestamp'] = datetime.now().isoformat()
        transformed_df['record_hash'] = [hash(str(row)) for row in transformed_df.values]

        logger.info(f"Transformed: {len(transformed_df)} records, {len(transformed_df.columns)} columns")
        return transformed_df

    def invoke_function(self, function_name, payload):
        invocation = {
            'function': function_name,
            'payload': payload,
            'timestamp': datetime.now(),
            'status': 'SUCCESS'
        }
        self.invocations.append(invocation)
        logger.info(f"Lambda invoked: {function_name}")
        return {'StatusCode': 200, 'Payload': {'result': 'success'}}

class AthenaSimulator:
    def __init__(self):
        self.tables = {}
        self.queries = []
        logger.info("Athena Simulator initialized")

    def store_data(self, df, table_name='pipeline_data'):
        self.tables[table_name] = df
        logger.info(f"Data stored in table: {table_name} ({len(df)} records)")

    def execute_query(self, query, table_name='pipeline_data'):
        if table_name not in self.tables:
            return pd.DataFrame()

        df = self.tables[table_name]

        # Simple query parsing
        if 'LIMIT' in query.upper():
            try:
                limit = int(query.upper().split('LIMIT')[1].strip().split()[0])
                df = df.head(limit)
            except:
                pass

        self.queries.append({
            'query': query,
            'timestamp': datetime.now(),
            'rows_returned': len(df)
        })

        logger.info(f"Query executed: {len(df)} rows returned")
        return df

class CloudWatchSimulator:
    def __init__(self):
        self.metrics = []
        self.alarms = []
        logger.info("CloudWatch Simulator initialized")

    def put_metric(self, namespace, metric_name, value, unit='Count'):
        metric = {
            'namespace': namespace,
            'metric_name': metric_name,
            'value': value,
            'unit': unit,
            'timestamp': datetime.now()
        }
        self.metrics.append(metric)
        logger.info(f"Metric logged: {metric_name} = {value}")

    def create_alarm(self, alarm_name, metric_name, threshold, comparison_operator):
        alarm = {
            'name': alarm_name,
            'metric': metric_name,
            'threshold': threshold,
            'operator': comparison_operator,
            'created': datetime.now()
        }
        self.alarms.append(alarm)
        logger.info(f"Alarm created: {alarm_name}")
        return alarm

class AWSSimulator:
    def __init__(self):
        self.s3 = S3Simulator()
        self.glue = GlueSimulator()
        self.lambda_service = LambdaSimulator()
        self.athena = AthenaSimulator()
        self.cloudwatch = CloudWatchSimulator()
        logger.info("AWSSimulator fully initialized with all services")

    def get_service_status(self):
        return {
            'S3': 'Active',
            'Glue': 'Active',
            'Lambda': 'Active',
            'Athena': 'Active',
            'CloudWatch': 'Active'
        }
