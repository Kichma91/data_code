from prefect import flow, get_run_logger, task
from sql_update.space_nk_sql_update import update_spacenk


@task(name="SpaceNK Update")
def spacenk_update(logger, sheets=None):
    """
    main task for updating spacenk_table. It is implemented into update_all_tables workflow
    :param logger: logger from main Prefect flow
    :param sheets: list of sheets we want to update. None = All sheets (['lw_store', 'fy_store']) - use this list as
                    reference if you want to choose sheets and not update all sheets
    """

    logger.info("SpaceNK Update task running")
    updated_table_messages = update_spacenk(save_files=True, sheets=sheets)
    logger.info(f"SpaceNK update finished:")
    # for each updated table sheet, logger will broadcast a message
    for message in updated_table_messages:
        logger.info(message)

@flow(name="Update Excel tables")
def update_all_tables(space_nk=True, space_nk_sheets=None):
    """
    main workflow for updating all tables.
    :param space_nk: choose if you want to update the spaceNK file. If we had other files, we would add them aswell
    :param space_nk_sheets: list of sheets we want to update in spaceNK file. None = All sheets (['lw_store', 'fy_store']) -
                            use this list as reference if you want to choose sheets and not update all sheets
    :return:
    """
    logger = get_run_logger()
    logger.info("Uploading all SQL files")
    # if we had multiple Excel file, there would be an if statement for each of them
    if space_nk:
        spacenk_update(logger, sheets=space_nk_sheets)
    logger.info("Finished uploading all SQL excel files")



if __name__ == "__main__":
    update_all_tables()
