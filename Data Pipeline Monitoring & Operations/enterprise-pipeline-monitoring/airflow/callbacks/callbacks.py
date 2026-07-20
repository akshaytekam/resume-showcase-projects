from utils.logger import logger


def success_callback(context):

    task=context["task_instance"]

    logger.info(f"{task.task_id} SUCCESS")


def failure_callback(context):

    task=context["task_instance"]

    logger.error(f"{task.task_id} FAILED")
