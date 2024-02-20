from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
    AssetSelection
)
from . import assets


training_job = define_asset_job(
    "training_job", AssetSelection.groups("TrainingPhase")
    )

prod_job = define_asset_job(
    "prod_job", AssetSelection.groups("VersioningPhase", "DataCollectionPhase", "ModelPhase", "MonitoringPhase" )
    )


train_schedule = ScheduleDefinition(
    job=training_job, cron_schedule="0 8 * * *"
    )


prod_schedule = ScheduleDefinition(
    job=prod_job, cron_schedule="0 19 * * 1-5"
    )


defs = Definitions(
    assets=load_assets_from_package_module(assets), schedules=[train_schedule, prod_schedule ]
)
