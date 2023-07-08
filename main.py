from prefect import flow, get_run_logger
from sql_update.space_nk_sql_update import update_spaceNK

@flow(name="SpaceNK Update")
def SpaceNK_update():
    logger = get_run_logger()
    logger.warning("SpaceNK Update flow running")
    lw_store_len, fy_store_len = update_spaceNK()
    logger.info(f"SpaceNK update finished:")
    logger.info(f"Last Week per store : uploaded {lw_store_len} rows")
    logger.info(f"Fiscal Year  per store : uploaded {fy_store_len} rows")


if __name__ == "__main__":
    SpaceNK_update()