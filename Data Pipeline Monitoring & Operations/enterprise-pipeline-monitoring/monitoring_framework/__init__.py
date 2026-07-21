"""
===============================================================================

Package Name

    monitoring_framework


Description

    Enterprise Data Pipeline Monitoring and Operations Framework


Modules

    monitoring
        Pipeline monitoring and metrics collection


    validation
        Data quality validation framework


    alerting
        Incident management and notifications


    reporting
        Operational reporting and dashboards


    pipelines
        Data pipeline implementations


Author

    Enterprise Data Engineering Team


===============================================================================
"""


###############################################################################
# Package Metadata
###############################################################################

__package_name__ = "monitoring_framework"

__version__ = "1.0.0"

__author__ = "Data Engineering Team"


###############################################################################
# Framework Information
###############################################################################

FRAMEWORK_NAME = (

    "Enterprise Data Pipeline "

    "Monitoring & Operations Framework"

)


ENVIRONMENT = "DEV"


###############################################################################
# Initialization Message
###############################################################################

def framework_info():
    """
    Return framework information.
    """

    return {


        "name":

            FRAMEWORK_NAME,


        "version":

            __version__,


        "environment":

            ENVIRONMENT


    }
