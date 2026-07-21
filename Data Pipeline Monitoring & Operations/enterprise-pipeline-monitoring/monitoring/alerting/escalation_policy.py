"""
===============================================================================

File Name
    escalation_policy.py

Description
    Enterprise Incident Escalation Policy

Features

    • Time-Based Escalation
    • Severity Rules
    • Multi-Level Support
    • Escalation Tracking

Author
    Enterprise Data Engineering Team

===============================================================================
"""

from datetime import datetime

from alerting.alert_config import (

    WARNING_DELAY,

    ERROR_DELAY,

    CRITICAL_DELAY

)

from monitoring.monitoring_logger import (

    log_info,

    log_error

)

###############################################################################
# Escalation Policy
###############################################################################

class EscalationPolicy:

    """
    Enterprise escalation policy.
    """

    def __init__(self):

        self.history = []

###############################################################################
# Escalation Matrix
###############################################################################

    def get_escalation_levels(self):
        """
        Return enterprise escalation matrix.
        """

        return {

            "WARNING": [

                {

                    "level": 1,

                    "delay": WARNING_DELAY,

                    "group": "Data Engineering Support"

                }

            ],

            "ERROR": [

                {

                    "level": 1,

                    "delay": ERROR_DELAY,

                    "group": "L2 Data Engineering"

                },

                {

                    "level": 2,

                    "delay": 30,

                    "group": "Data Engineering Manager"

                }

            ],

            "CRITICAL": [

                {

                    "level": 1,

                    "delay": CRITICAL_DELAY,

                    "group": "L2 Data Engineering"

                },

                {

                    "level": 2,

                    "delay": 30,

                    "group": "Data Engineering Manager"

                },

                {

                    "level": 3,

                    "delay": 60,

                    "group": "Engineering Director"

                }

            ]

        }

###############################################################################
# Get Policy
###############################################################################

    def get_policy(
        self,
        severity
    ):
        """
        Return escalation policy.
        """

        matrix = self.get_escalation_levels()

        return matrix.get(

            severity,

            []

        )

###############################################################################
# Current Escalation Level
###############################################################################

    def determine_level(
        self,
        severity,
        incident_age_minutes
    ):
        """
        Determine escalation level
        based on incident age.
        """

        levels = self.get_policy(

            severity

        )

        current = None

        for level in levels:

            if incident_age_minutes >= level["delay"]:

                current = level

        return current


###############################################################################
# Already Escalated?
###############################################################################

    def is_already_escalated(
        self,
        incident_number,
        level
    ):
        """
        Check whether this incident has already
        been escalated to the given level.
        """

        for record in self.history:

            if (

                record["incident_number"] == incident_number

                and record["level"] == level

            ):

                return True

        return False

###############################################################################
# Record Escalation
###############################################################################

    def record_escalation(
        self,
        incident_number,
        level,
        target_group
    ):
        """
        Save escalation history.
        """

        self.history.append(

            {

                "incident_number": incident_number,

                "level": level,

                "target_group": target_group,

                "timestamp": datetime.utcnow()

            }

        )

        log_info(

            f"{incident_number} escalated "

            f"to Level {level}"

        )

###############################################################################
# Execute Escalation
###############################################################################

    def execute_escalation(
        self,
        incident
    ):
        """
        Execute escalation policy.
        """

        created = incident["created_time"]

        age = (

            datetime.utcnow() - created

        ).total_seconds() / 60

        escalation = self.determine_level(

            incident["severity"],

            age

        )

        if escalation is None:

            return None

        if self.is_already_escalated(

            incident["incident_number"],

            escalation["level"]

        ):

            log_info(

                f"{incident['incident_number']} "

                f"already escalated."

            )

            return escalation

        self.record_escalation(

            incident["incident_number"],

            escalation["level"],

            escalation["group"]

        )

        log_info(

            f"Escalating "

            f"{incident['incident_number']} "

            f"to "

            f"{escalation['group']}"

        )

        #
        # Integration hooks
        #

        # email.send_critical(...)

        # slack.send_critical(...)

        # teams.send_critical(...)

        # incident_manager.update_status(...)

        return escalation

###############################################################################
# Send Reminder
###############################################################################

    def send_reminder(
        self,
        incident
    ):
        """
        Send reminder for unresolved incident.
        """

        log_info(

            f"Reminder sent for "

            f"{incident['incident_number']}"

        )

        #
        # Future integrations:
        #
        # email.send_warning(...)
        # slack.send_warning(...)
        # teams.send_warning(...)
        #


###############################################################################
# Get Escalation History
###############################################################################

    def get_history(self):
        """
        Return complete escalation history.
        """

        return self.history

###############################################################################
# Get Incident Escalation History
###############################################################################

    def get_incident_history(
        self,
        incident_number
    ):
        """
        Return escalation history
        for one incident.
        """

        return [

            record

            for record in self.history

            if record["incident_number"] == incident_number

        ]

###############################################################################
# Escalation Statistics
###############################################################################

    def build_statistics(self):
        """
        Build escalation statistics.
        """

        stats = {

            "total_escalations": len(self.history),

            "level_1": 0,

            "level_2": 0,

            "level_3": 0

        }

        for record in self.history:

            level = f"level_{record['level']}"

            if level in stats:

                stats[level] += 1

        return stats

###############################################################################
# Build Dashboard Summary
###############################################################################

    def build_summary(self):
        """
        Build escalation dashboard summary.
        """

        statistics = self.build_statistics()

        return {

            "total_escalations":

                statistics["total_escalations"],

            "level_1":

                statistics["level_1"],

            "level_2":

                statistics["level_2"],

            "level_3":

                statistics["level_3"],

            "history_size":

                len(self.history)

        }

###############################################################################
# Archive History
###############################################################################

    def archive_history(self):
        """
        Archive escalation history.

        In production this would
        write to a database.
        """

        archived = self.history.copy()

        self.history.clear()

        log_info(

            f"{len(archived)} escalation "

            f"records archived."

        )

        return archived

###############################################################################
# Shutdown
###############################################################################

    def close(self):
        """
        Shutdown escalation engine.
        """

        log_info(

            "Escalation policy stopped."

        )
