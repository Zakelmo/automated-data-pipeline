from datetime import datetime
from enum import Enum

class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class Alert:
    def __init__(self, severity, message, source="Pipeline"):
        self.severity = severity
        self.message = message
        self.source = source
        self.timestamp = datetime.now()
        self.acknowledged = False

    def acknowledge(self):
        self.acknowledged = True

    def to_dict(self):
        return {
            'severity': self.severity.value,
            'message': self.message,
            'source': self.source,
            'timestamp': self.timestamp.isoformat(),
            'acknowledged': self.acknowledged
        }

class AlertManager:
    def __init__(self):
        self.alerts = []

    def create_alert(self, severity, message, source="Pipeline"):
        alert = Alert(severity, message, source)
        self.alerts.append(alert)
        return alert

    def get_active_alerts(self):
        return [a for a in self.alerts if not a.acknowledged]

    def get_all_alerts(self):
        return self.alerts

    def acknowledge_alert(self, alert):
        alert.acknowledge()

    def clear_acknowledged(self):
        self.alerts = [a for a in self.alerts if not a.acknowledged]
