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

trade_job = define_asset_job(
    "trade_job", AssetSelection.groups("TradingPhase")
)

prod_schedule = ScheduleDefinition(
    job=prod_job, cron_schedule="0 19 * * 1-5"
    )

defs = Definitions(
    assets=load_assets_from_package_module(assets), schedules=[train_schedule, prod_schedule, serve_schedule, trade_schedule ]
)
