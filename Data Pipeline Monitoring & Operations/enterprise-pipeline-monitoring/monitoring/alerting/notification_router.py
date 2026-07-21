"""
===============================================================================

File Name
    notification_router.py

Description
    Enterprise Notification Routing Engine

Features

    • Alert Routing
    • Severity Management
    • Channel Coordination
    • Incident Triggering
    • Escalation Integration

Author
    Enterprise Data Engineering Team

===============================================================================
"""


from datetime import datetime


from alerting.email_alert import EmailAlert

from alerting.slack_notifier import SlackNotifier

from alerting.teams_notifier import TeamsNotifier

from alerting.incident_manager import IncidentManager

from alerting.escalation_policy import EscalationPolicy


from monitoring.monitoring_logger import (

    log_info,

    log_error

)


###############################################################################
# Notification Router
###############################################################################

class NotificationRouter:

    """
    Central alert routing engine.
    """


    def __init__(self):

        self.email = EmailAlert()

        self.slack = SlackNotifier()

        self.teams = TeamsNotifier()

        self.incident_manager = IncidentManager()

        self.escalation = EscalationPolicy()


        self.processed_alerts = []


###############################################################################
# Severity Validation
###############################################################################

    def validate_severity(
        self,
        severity
    ):
        """
        Validate alert severity.
        """

        allowed = [

            "INFO",

            "WARNING",

            "ERROR",

            "CRITICAL"

        ]


        if severity not in allowed:

            raise ValueError(

                f"Invalid severity {severity}"

            )


        return True


###############################################################################
# Build Alert Metadata
###############################################################################

    def build_metadata(
        self,
        alert
    ):
        """
        Add operational metadata.
        """

        alert["timestamp"] = datetime.utcnow()


        alert["processed"] = False


        return alert


###############################################################################
# Duplicate Alert Check
###############################################################################

    def is_duplicate_alert(
        self,
        alert
    ):
        """
        Check whether alert was already processed.
        """

        alert_key = (

            alert["pipeline"],

            alert["title"],

            alert["severity"]

        )


        for processed in self.processed_alerts:

            existing_key = (

                processed["pipeline"],

                processed["title"],

                processed["severity"]

            )


            if alert_key == existing_key:

                return True


        return False


###############################################################################
# Store Alert History
###############################################################################

    def store_alert_history(
        self,
        alert
    ):
        """
        Store processed alert.
        """

        self.processed_alerts.append(

            alert

        )


        log_info(

            f"Alert stored: {alert['title']}"

        )



###############################################################################
# Determine Routing Rules
###############################################################################

    def get_routing_policy(
        self,
        severity
    ):
        """
        Return notification routing rules.
        """

        policy = {


            "INFO": {

                "email": False,

                "slack": True,

                "teams": True,

                "incident": False

            },


            "WARNING": {

                "email": True,

                "slack": True,

                "teams": True,

                "incident": False

            },


            "ERROR": {

                "email": True,

                "slack": True,

                "teams": True,

                "incident": True

            },


            "CRITICAL": {

                "email": True,

                "slack": True,

                "teams": True,

                "incident": True

            }


        }


        return policy.get(

            severity

        )


###############################################################################
# Route Alert
###############################################################################

    def route_alert(
        self,
        alert
    ):
        """
        Main alert routing method.
        """


        severity = alert["severity"]


        self.validate_severity(

            severity

        )


        alert = self.build_metadata(

            alert

        )


        if self.is_duplicate_alert(

            alert

        ):

            log_info(

                "Duplicate alert ignored."

            )

            return {

                "status":

                    "DUPLICATE"

            }


        routing = self.get_routing_policy(

            severity

        )


        if routing["email"]:

            self.email.send(

                severity=severity,

                title=alert["title"],

                message=alert["message"]

            )


        if routing["slack"]:

            self.slack.send(

                severity=severity,

                title=alert["title"],

                message=alert["message"]

            )


        if routing["teams"]:

            self.teams.send(

                severity=severity,

                title=alert["title"],

                message=alert["message"]

            )



        incident = None


        if routing["incident"]:


            incident = self.incident_manager.create_incident(

                severity=severity,

                title=alert["title"],

                description=alert["message"]

            )



        self.store_alert_history(

            alert

        )


        return {


            "status":

                "PROCESSED",


            "incident":

                incident

        }


###############################################################################
# Trigger Escalation
###############################################################################

    def trigger_escalation(
        self,
        incident
    ):
        """
        Trigger escalation workflow.
        """

        if incident is None:

            return None


        escalation = self.escalation.execute_escalation(

            incident

        )


        if escalation:

            log_info(

                f"Escalation triggered for "

                f"{incident['incident_number']}"

            )


        return escalation


###############################################################################
# Process Critical Incident
###############################################################################

    def process_incident(
        self,
        incident
    ):
        """
        Process newly created incident.
        """

        if incident is None:

            return False


        self.trigger_escalation(

            incident

        )


        return True


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

        for alert in self.processed_alerts:


            if alert.get("alert_id") == alert_id:


                alert["acknowledged"] = True

                alert["acknowledged_by"] = engineer

                alert["acknowledged_time"] = datetime.utcnow()


                log_info(

                    f"Alert {alert_id} acknowledged "

                    f"by {engineer}"

                )


                return True


        log_error(

            f"Alert not found: {alert_id}"

        )


        return False


###############################################################################
# Notification Statistics
###############################################################################

    def build_statistics(
        self
    ):
        """
        Build notification metrics.
        """

        statistics = {


            "total_alerts": len(

                self.processed_alerts

            ),


            "INFO": 0,


            "WARNING": 0,


            "ERROR": 0,


            "CRITICAL": 0,


            "acknowledged": 0

        }


        for alert in self.processed_alerts:


            severity = alert["severity"]


            if severity in statistics:

                statistics[severity] += 1



            if alert.get(

                "acknowledged",

                False

            ):

                statistics["acknowledged"] += 1


        return statistics



###############################################################################
# Dashboard Summary
###############################################################################

    def build_dashboard_summary(
        self
    ):
        """
        Build monitoring dashboard data.
        """

        stats = self.build_statistics()


        return {


            "application":

                "Data Pipeline Monitoring Framework",


            "generated_time":

                datetime.utcnow(),


            "notifications":

                stats,


            "active_alerts":

                len(

                    self.processed_alerts

                )

        }


###############################################################################
# Cleanup Alerts
###############################################################################

    def cleanup_processed_alerts(
        self
    ):
        """
        Remove old processed alerts.

        Production:
        Archive to database/object storage.
        """

        archived = self.processed_alerts.copy()


        self.processed_alerts.clear()


        log_info(

            f"{len(archived)} alerts archived."

        )


        return archived



###############################################################################
# Shutdown Router
###############################################################################

    def close(
        self
    ):
        """
        Shutdown notification router.
        """

        self.email.close()

        self.slack.close()

        self.teams.close()

        self.incident_manager.close()

        self.escalation.close()


        log_info(

            "Notification Router stopped."

        )
