"""
===============================================================================

File Name
    alert_manager.py

Description
    Enterprise Alert Lifecycle Management Service

Features

    • Alert Creation
    • Alert Validation
    • Alert State Management
    • Notification Coordination
    • Incident Integration

Author
    Enterprise Data Engineering Team

===============================================================================
"""


from datetime import datetime

import uuid


from alerting.notification_router import (

    NotificationRouter

)


from alerting.incident_manager import (

    IncidentManager

)


from monitoring.monitoring_logger import (

    log_info,

    log_error

)



###############################################################################
# Alert Manager
###############################################################################

class AlertManager:

    """
    Central alert lifecycle controller.
    """



    def __init__(self):


        self.router = NotificationRouter()


        self.incident_manager = IncidentManager()


        self.alert_store = {}



###############################################################################
# Generate Alert ID
###############################################################################

    def generate_alert_id(
        self
    ):
        """
        Generate unique alert identifier.
        """

        return (

            "ALERT-"

            +

            str(

                uuid.uuid4()

            )[:8].upper()

        )



###############################################################################
# Validate Alert
###############################################################################

    def validate_alert(
        self,
        alert
    ):
        """
        Validate alert payload.
        """

        required_fields = [

            "pipeline",

            "severity",

            "title",

            "message"

        ]


        for field in required_fields:


            if field not in alert:


                raise ValueError(

                    f"Missing field: {field}"

                )


        return True



###############################################################################
# Create Alert Object
###############################################################################

    def create_alert(
        self,
        alert_data
    ):
        """
        Create alert lifecycle object.
        """


        self.validate_alert(

            alert_data

        )


        alert_id = self.generate_alert_id()



        alert = {


            "alert_id":

                alert_id,


            "pipeline":

                alert_data["pipeline"],


            "severity":

                alert_data["severity"],


            "title":

                alert_data["title"],


            "message":

                alert_data["message"],


            "status":

                "NEW",


            "created_time":

                datetime.utcnow(),


            "updated_time":

                datetime.utcnow()


        }



        self.alert_store[

            alert_id

        ] = alert



        log_info(

            f"Alert created {alert_id}"

        )


        return alert


###############################################################################
# Get Alert
###############################################################################

    def get_alert(
        self,
        alert_id
    ):
        """
        Retrieve alert details.
        """

        return self.alert_store.get(

            alert_id

        )


###############################################################################
# Update Alert Status
###############################################################################

    def update_status(
        self,
        alert_id,
        status
    ):
        """
        Update alert lifecycle status.
        """

        alert = self.get_alert(

            alert_id

        )


        if alert is None:


            log_error(

                f"Alert not found: {alert_id}"

            )


            return False



        alert["status"] = status


        alert["updated_time"] = datetime.utcnow()



        log_info(

            f"{alert_id} status changed to {status}"

        )


        return True



###############################################################################
# Process Alert
###############################################################################

    def process_alert(
        self,
        alert_id
    ):
        """
        Complete alert processing workflow.
        """

        alert = self.get_alert(

            alert_id

        )


        if alert is None:


            log_error(

                f"Alert not found: {alert_id}"

            )


            return None



        self.update_status(

            alert_id,

            "PROCESSING"

        )


        response = self.router.route_alert(

            alert

        )



        if response["status"] == "PROCESSED":


            self.update_status(

                alert_id,

                "NOTIFIED"

            )


        if response.get(

            "incident"

        ):


            alert["incident"] = response["incident"]



            self.update_status(

                alert_id,

                "INCIDENT_CREATED"

            )



        log_info(

            f"Alert processing completed {alert_id}"

        )


        return response



###############################################################################
# Acknowledge Alert
###############################################################################

    def acknowledge_alert(
        self,
        alert_id,
        engineer
    ):
        """
        Mark alert as acknowledged.
        """

        alert = self.get_alert(

            alert_id

        )


        if alert is None:


            return False



        alert["status"] = "ACKNOWLEDGED"


        alert["acknowledged_by"] = engineer


        alert["acknowledged_time"] = datetime.utcnow()


        alert["updated_time"] = datetime.utcnow()



        log_info(

            f"{alert_id} acknowledged by {engineer}"

        )


        return True



###############################################################################
# Resolve Alert
###############################################################################

    def resolve_alert(
        self,
        alert_id,
        resolution
    ):
        """
        Resolve alert.
        """

        alert = self.get_alert(

            alert_id

        )


        if alert is None:


            return False



        alert["status"] = "RESOLVED"


        alert["resolution"] = resolution


        alert["resolved_time"] = datetime.utcnow()


        alert["updated_time"] = datetime.utcnow()



        log_info(

            f"{alert_id} resolved"

        )


        return True


###############################################################################
# Get Active Alerts
###############################################################################

    def get_active_alerts(
        self
    ):
        """
        Return alerts that are not resolved.
        """

        active_states = [

            "NEW",

            "PROCESSING",

            "NOTIFIED",

            "INCIDENT_CREATED",

            "ACKNOWLEDGED"

        ]


        return [

            alert

            for alert in self.alert_store.values()

            if alert["status"] in active_states

        ]



###############################################################################
# Get Alerts By Severity
###############################################################################

    def get_alerts_by_severity(
        self,
        severity
    ):
        """
        Return alerts filtered by severity.
        """

        return [

            alert

            for alert in self.alert_store.values()

            if alert["severity"] == severity

        ]



###############################################################################
# Build Alert Statistics
###############################################################################

    def build_statistics(
        self
    ):
        """
        Generate alert statistics.
        """

        statistics = {


            "total_alerts":

                len(self.alert_store),


            "NEW":

                0,


            "PROCESSING":

                0,


            "NOTIFIED":

                0,


            "INCIDENT_CREATED":

                0,


            "ACKNOWLEDGED":

                0,


            "RESOLVED":

                0,


            "CRITICAL":

                0,


            "ERROR":

                0,


            "WARNING":

                0,


            "INFO":

                0

        }


        for alert in self.alert_store.values():


            status = alert["status"]


            severity = alert["severity"]



            if status in statistics:

                statistics[status] += 1



            if severity in statistics:

                statistics[severity] += 1



        return statistics



###############################################################################
# Calculate SLA Metrics
###############################################################################

    def calculate_sla_metrics(
        self
    ):
        """
        Calculate operational SLA metrics.
        """

        total = len(

            self.alert_store

        )


        resolved = [

            alert

            for alert in self.alert_store.values()

            if alert["status"] == "RESOLVED"

        ]


        resolution_times = []



        for alert in resolved:


            if (

                "created_time" in alert

                and

                "resolved_time" in alert

            ):


                duration = (

                    alert["resolved_time"]

                    -

                    alert["created_time"]

                ).total_seconds() / 60



                resolution_times.append(

                    duration

                )



        average_resolution_time = 0


        if resolution_times:


            average_resolution_time = (

                sum(resolution_times)

                /

                len(resolution_times)

            )



        return {


            "total_alerts":

                total,


            "resolved_alerts":

                len(resolved),


            "average_resolution_minutes":

                round(

                    average_resolution_time,

                    2

                )

        }



###############################################################################
# Dashboard Summary
###############################################################################

    def build_dashboard_summary(
        self
    ):
        """
        Create monitoring dashboard payload.
        """

        return {


            "application":

                "Data Pipeline Monitoring Framework",


            "generated_time":

                datetime.utcnow(),


            "statistics":

                self.build_statistics(),


            "sla_metrics":

                self.calculate_sla_metrics(),


            "active_alert_count":

                len(

                    self.get_active_alerts()

                )

        }



###############################################################################
# Archive Resolved Alerts
###############################################################################

    def archive_resolved_alerts(
        self
    ):
        """
        Archive resolved alerts.

        Production:
        Move records to database/object storage.
        """

        archived = []


        for alert_id in list(

            self.alert_store.keys()

        ):


            alert = self.alert_store[

                alert_id

            ]



            if alert["status"] == "RESOLVED":


                archived.append(

                    alert

                )


                del self.alert_store[

                    alert_id

                ]



        log_info(

            f"{len(archived)} alerts archived."

        )


        return archived



###############################################################################
# Shutdown
###############################################################################

    def close(
        self
    ):
        """
        Shutdown Alert Manager.
        """

        self.router.close()


        self.incident_manager.close()


        log_info(

            "Alert Manager stopped."

        )
