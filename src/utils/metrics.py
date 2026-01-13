from datetime import datetime
import pandas as pd

class PipelineMetrics:
    def __init__(self):
        self.runs = []
        self.total_records = 0

    def add_run(self, duration, success, records=0):
        self.runs.append({
            'timestamp': datetime.now(),
            'duration': duration,
            'success': success,
            'records': records
        })
        if success:
            self.total_records += records

    def get_total_runs(self):
        return len(self.runs)

    def get_success_rate(self):
        if not self.runs:
            return 100.0
        successes = sum(1 for run in self.runs if run['success'])
        return (successes / len(self.runs)) * 100

    def get_avg_duration(self):
        if not self.runs:
            return 0.0
        return sum(run['duration'] for run in self.runs) / len(self.runs)

    def get_total_records(self):
        return self.total_records

    def get_metrics_df(self):
        if not self.runs:
            return pd.DataFrame()
        return pd.DataFrame(self.runs)

class DataQualityChecker:
    def __init__(self):
        pass

    def check_quality(self, df):
        issues = []

        # Completeness
        total_cells = df.shape[0] * df.shape[1]
        missing_cells = df.isnull().sum().sum()
        completeness = ((total_cells - missing_cells) / total_cells) * 100

        if missing_cells > 0:
            issues.append(f"Found {missing_cells} missing values")

        # Validity (check for negative values in numeric columns)
        validity = 100.0
        for col in df.select_dtypes(include=['number']).columns:
            if (df[col] < 0).any():
                issues.append(f"Column '{col}' contains negative values")
                validity -= 5

        # Uniqueness
        duplicate_rows = df.duplicated().sum()
        uniqueness = ((df.shape[0] - duplicate_rows) / df.shape[0]) * 100

        if duplicate_rows > 0:
            issues.append(f"Found {duplicate_rows} duplicate rows")

        return {
            'completeness': completeness,
            'validity': max(validity, 0),
            'uniqueness': uniqueness,
            'issues': issues,
            'overall_score': (completeness + validity + uniqueness) / 3
        }
