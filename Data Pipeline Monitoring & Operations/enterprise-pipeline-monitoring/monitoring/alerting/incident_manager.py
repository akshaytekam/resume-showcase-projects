"""
===============================================================================

File Name
    incident_manager.py

Description
    Enterprise Incident Management Service

Features

    • Incident Creation
    • Incident Tracking
    • Severity Classification
    • Assignment Groups
    • Status Management

Author
    Enterprise Data Engineering Team

===============================================================================
"""

import uuid

from datetime import datetime

from alerting.alert_config import (

    INCIDENT_ENABLED,

    INCIDENT_SYSTEM,

    DEFAULT_ASSIGNMENT_GROUP,

    APPLICATION_NAME,

    ENVIRONMENT

)

from monitoring.monitoring_logger import (

    log_info,

    log_error

)

###############################################################################
# Incident Manager
###############################################################################

class IncidentManager:

    """
    Enterprise Incident Manager.
    """

    def __init__(self):

        self.active_incidents = {}

###############################################################################
# Generate Incident Number
###############################################################################

    def generate_incident_number(
        self
    ):
        """
        Generate enterprise-style incident number.
        """

        unique = str(

            uuid.uuid4()

        )[:8].upper()

        return f"INC-{unique}"

###############################################################################
# Create Incident Object
###############################################################################

    def create_incident(
        self,
        severity,
        title,
        description,
        assignment_group=DEFAULT_ASSIGNMENT_GROUP
    ):
        """
        Create a new incident.
        """

        if not INCIDENT_ENABLED:

            log_info(

                "Incident creation disabled."

            )

            return None

        incident_number = self.generate_incident_number()

        incident = {

            "incident_number": incident_number,

            "system": INCIDENT_SYSTEM,

            "application": APPLICATION_NAME,

            "environment": ENVIRONMENT,

            "severity": severity,

            "title": title,

            "description": description,

            "status": "OPEN",

            "assignment_group": assignment_group,

            "created_time": datetime.utcnow(),

            "updated_time": datetime.utcnow()

        }

        self.active_incidents[

            incident_number

        ] = incident

        log_info(

            f"Incident created: {incident_number}"

        )

        return incident


###############################################################################
# Get Incident
###############################################################################

    def get_incident(
        self,
        incident_number
    ):
        """
        Return incident details.
        """

        return self.active_incidents.get(

            incident_number

        )

###############################################################################
# Update Incident Status
###############################################################################

    def update_status(
        self,
        incident_number,
        status
    ):
        """
        Update incident status.
        """

        incident = self.get_incident(

            incident_number

        )

        if incident is None:

            log_error(

                f"Incident not found: {incident_number}"

            )

            return False

        incident["status"] = status

        incident["updated_time"] = datetime.utcnow()

        log_info(

            f"{incident_number} updated to {status}"

        )

        return True

###############################################################################
# Assign Incident
###############################################################################

    def assign_incident(
        self,
        incident_number,
        assignee
    ):
        """
        Assign incident to an engineer.
        """

        incident = self.get_incident(

            incident_number

        )

        if incident is None:

            log_error(

                f"Incident not found: {incident_number}"

            )

            return False

        incident["assignee"] = assignee

        incident["status"] = "ASSIGNED"

        incident["updated_time"] = datetime.utcnow()

        log_info(

            f"{incident_number} assigned to {assignee}"

        )

        return True

###############################################################################
# Resolve Incident
###############################################################################

    def resolve_incident(
        self,
        incident_number,
        resolution_notes
    ):
        """
        Resolve an incident.
        """

        incident = self.get_incident(

            incident_number

        )

        if incident is None:

            log_error(

                f"Incident not found: {incident_number}"

            )

            return False

        incident["status"] = "RESOLVED"

        incident["resolution_notes"] = resolution_notes

        incident["resolved_time"] = datetime.utcnow()

        incident["updated_time"] = datetime.utcnow()

        log_info(

            f"{incident_number} resolved."

        )

        return True

###############################################################################
# Close Incident
###############################################################################

    def close_incident(
        self,
        incident_number
    ):
        """
        Close an incident.
        """

        incident = self.get_incident(

            incident_number

        )

        if incident is None:

            log_error(

                f"Incident not found: {incident_number}"

            )

            return False

        incident["status"] = "CLOSED"

        incident["closed_time"] = datetime.utcnow()

        incident["updated_time"] = datetime.utcnow()

        log_info(

            f"{incident_number} closed."

        )

        return True


###############################################################################
# Get Active Incidents
###############################################################################

    def get_active_incidents(self):
        """
        Return all active incidents.
        """

        active_status = {

            "OPEN",

            "ASSIGNED",

            "IN_PROGRESS"

        }

        return [

            incident

            for incident in self.active_incidents.values()

            if incident["status"] in active_status

        ]

###############################################################################
# Search by Severity
###############################################################################

    def get_incidents_by_severity(
        self,
        severity
    ):
        """
        Return incidents of a specific severity.
        """

        return [

            incident

            for incident in self.active_incidents.values()

            if incident["severity"] == severity

        ]

###############################################################################
# Duplicate Incident Detection
###############################################################################

    def find_duplicate_incident(
        self,
        title
    ):
        """
        Find an existing active incident
        having the same title.
        """

        for incident in self.get_active_incidents():

            if incident["title"] == title:

                return incident

        return None

###############################################################################
# Incident Statistics
###############################################################################

    def build_statistics(self):
        """
        Build incident statistics.
        """

        incidents = list(

            self.active_incidents.values()

        )

        statistics = {

            "total_incidents": len(incidents),

            "open": 0,

            "assigned": 0,

            "in_progress": 0,

            "resolved": 0,

            "closed": 0,

            "critical": 0,

            "error": 0,

            "warning": 0,

            "info": 0

        }

        for incident in incidents:

            status = incident["status"].lower()

            severity = incident["severity"].lower()

            if status in statistics:

                statistics[status] += 1

            if severity in statistics:

                statistics[severity] += 1

        return statistics

###############################################################################
# Build Operational Summary
###############################################################################

    def build_summary(self):
        """
        Build incident dashboard summary.
        """

        stats = self.build_statistics()

        return {

            "incident_system": INCIDENT_SYSTEM,

            "application": APPLICATION_NAME,

            "environment": ENVIRONMENT,

            "statistics": stats,

            "active_incidents":

                len(

                    self.get_active_incidents()

                )

        }

###############################################################################
# Archive Closed Incidents
###############################################################################

    def archive_closed_incidents(self):
        """
        Remove closed incidents from memory.

        In production this would archive
        them to a database.
        """

        archived = []

        for incident_number in list(

            self.active_incidents.keys()

        ):

            incident = self.active_incidents[

                incident_number

            ]

            if incident["status"] == "CLOSED":

                archived.append(

                    incident

                )

                del self.active_incidents[

                    incident_number

                ]

        log_info(

            f"{len(archived)} incidents archived."

        )

        return archived

###############################################################################
# Shutdown
###############################################################################

    def close(self):
        """
        Shutdown Incident Manager.
        """

        log_info(

            "Incident Manager stopped."

        )


