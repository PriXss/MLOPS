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
    "prod_job", AssetSelection.groups("VersioningPhase", "DataCollectionPhase", "ModelPhase", "MonitoringPhase", "StockTrading" )
    )

serve_job = define_asset_job(
    "serve_job", AssetSelection.groups("ServingPhase")
)


train_schedule = ScheduleDefinition(
    job=training_job, cron_schedule="0 8 * * *"
    )


prod_schedule = ScheduleDefinition(
    job=prod_job, cron_schedule="0 19 * * 1-5"
    )

serve_schedule = ScheduleDefinition(
    job=serve_job, cron_schedule="0 19 * * 1-5"
)


defs = Definitions(
    assets=load_assets_from_package_module(assets), schedules=[train_schedule, prod_schedule, serve_schedule ]
)
