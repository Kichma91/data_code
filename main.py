import json

from prefect import flow, get_run_logger, task

from sql_update.space_nk_sql_update import update_spacenk


@task(name="SpaceNK Update")
def spacenk_update(logger, sheets=None):
    """
    main task for updating spacenk_table. It is implemented into update_all_tables workflow
    :param logger: prefect.get_run_logger() object - logger from main Prefect flow
    :param sheets: list(str) - list of sheets we want to update. None = All sheets (['lw_store', 'fy_store']) - use this
                              list as reference if you want to choose sheets and not update all sheets
    """

    logger.info("SpaceNK Update task running")
    # calling function from space_nk_sql_update file and receiving the messages
    updated_table_messages = update_spacenk(save_files=True, sheets=sheets)

    # for each updated table sheet, logger will broadcast a message. Here we are also raising an error in case file is
    # not found(error is first handled in space_nk_sql_update.update_spacenk function)
    for message in updated_table_messages:
        if message[:5] == "ERROR":
            raise FileNotFoundError(message)
        logger.info(message)

    logger.info(f"SpaceNK update finished:")


@flow(name="Update Excel tables")
def update_all_tables(space_nk=True, space_nk_sheets=None):
    """
    main workflow for updating all tables. it was set up so other Excel files can be added more easily
    :param space_nk: bool - choose if you want to update the spaceNK file. If we had other files, we would add them aswell
    :param space_nk_sheets: bool - list of sheets we want to update in spaceNK file. None = All sheets (['lw_store', 'fy_store']) -
                            use this list as reference if you want to choose sheets and not update all sheets
    """
    logger = get_run_logger()
    logger.info("Uploading all Excel files to SQL tables")
    # if we had multiple Excel file, there would be an if statement for each of them
    if space_nk:
        spacenk_update(logger, sheets=space_nk_sheets)
    logger.info("Finished uploading all the data")


if __name__ == "__main__":
    with open('config.json', 'r') as fp:
        config_data = json.load(fp)

    update_all_tables(space_nk=config_data['space_nk_update'], space_nk_sheets=config_data['space_nk_sheets'])
