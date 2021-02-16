from datetime import timedelta

CELERY_IMPORTS = ("tasks",)
CELERY_IGNORE_RESULT = False
BROKER_URL = "amqp://rabbitmq:5672"
CELERY_TIMEZONE = "Europe/Berlin"

CELERYBEAT_SCHEDULE = {

    "collect_supernode_data": {
        "task": "tasks.collect_supernode_data",
        "schedule": timedelta(minutes=5),
    },

}
