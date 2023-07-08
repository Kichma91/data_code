import json
import glob

import pandas as pd

from sqlalchemy import create_engine

from base_files.space_nk_base_files import spaceNK_base, lw_store_table_name, fy_store_table_name

"""
Besides reading first sheet, I also added code for 2nd sheet. Also, looking at the other
tables I think the code would be mostly a variation of these 2 codes below.
The code was tested on my PC and successfully uploaded the data to PostGreSQL server
"""


def last_week_store(file, update_closed_stores=False, save_files=False):
    """
    function that reads first sheet and returns transformed data
    :param file: str - Excel file we are reading
    :param update_closed_stores: bool - there is data from closed stores. True if we want those stores included in update
                                  False if we want it filtered out
    :param save_files: bool - do we want to store the table also locally in Excel to check it
    :return: pandas.DataFrame object - returns transformed data(frame)
    """
    # reading excel, removing empty cols and rows, filling nan values with 0 and removing the subtotals
    df_lw_store = pd.read_excel(file, sheet_name="Last Week Report by Store",
                                skiprows=5).drop(columns=['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 4']).fillna(0)[:-1]
    # a small check if Excel structure changed.
    if df_lw_store.columns[0] != 'Store No' or df_lw_store.iloc[-1]['Store No'] in ['Total','LY %','LY Total'] or\
            pd.isnull(df_lw_store.iloc[-1]['Store No']):
        raise Exception("ERROR: Structure of the dataframe is not correct")


    if not update_closed_stores:
        # in case we want to remove closed stores
        df_lw_store = df_lw_store.loc[~df_lw_store['Store'].str.contains('CLOSED')].reset_index(drop=True)
    if save_files:
        df_lw_store.to_excel(r'test1.xlsx')
    return df_lw_store


def fiscal_year_store(file, save_files=False):
    """
    Reads 2nd sheet and transforms it into a raw dataframe (schema in base files)
    :param file: str - Excel file we are reading
    :param save_files: bool - do we want to store the table also locally in Excel to check it
    :return: pandas.DataFrame object - returns transformed data(frame)
    """
    # removing Nan columns while loading, and the first total(YTD) column
    df_fy_store = pd.read_excel(file, sheet_name="Fiscal Year Report by Store", skiprows=2).drop(
        columns=['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 4', 'Unnamed: 5'])
    # this piece of code finds the index of the next table that is containing last year data
    last_year_index = df_fy_store.loc[df_fy_store['Unnamed: 2'].fillna('null').str.contains('Year')].iloc[1].name
    # this filters out just the current year(hard coded supposing structure won't change)
    df_fy_store2 = df_fy_store[:last_year_index - 5].reset_index(drop=True)
    # This is separated last year table in case we would need it. In that case I would store the below
    # part in a separate function, so I could call it on both dataframes easily
    df_fy_store_yearminus = df_fy_store[last_year_index:-1].reset_index(drop=True)
    # here we find the subtotal columns. Reason why I evaded hard coding here because these orders can change
    x = 2

    cols_to_drop = []
    for value in df_fy_store2.iloc[2][2:].fillna(''):
        if value == '':
            cols_to_drop.append(x)
        x += 1
    # dropping the subtotal columns
    df_fy_store2 = df_fy_store2.drop(df_fy_store2.columns[cols_to_drop], axis=1)
    # storing fiscal year info for later filling of data
    fiscal_year = df_fy_store2.iloc[0]['Unnamed: 2'][-4:]
    # looping through the rows that have column names of months and week numbers to create new column names
    col_names = []
    month = ''
    for row_1, row_2 in zip(df_fy_store2.iloc[1].fillna(''), df_fy_store2.iloc[2].fillna('')):
        if 'store' in row_1.lower():
            col_names.append(row_1)
        else:
            if row_1:
                month = row_1.split(' - ')[1]
            week = row_2.split(' ')[1]
            col_names.append(f'{month}-{week}')
    # setting those column names onto row 2 because now we will delete the previous rows and headers
    df_fy_store2.iloc[2] = col_names
    df_fy_store2 = df_fy_store2.iloc[2:].reset_index(drop=True)
    # transposing the frame to remove the header row and transpose back
    df_fy_store3 = df_fy_store2.T.set_index(0).T
    # The only structural error im not catching here is if there are less empty rows between the two tables than it
    # should be. In that case, we will have dataframe that is missing some store data.
    # I would probably implement some hard coded number(and pull it from config file) on how many rows I need to have
    # assuming number of stores is not changing (frequently).
    if df_fy_store3.columns[0] != 'Store No' or df_fy_store3.iloc[-1]['Store No'] in ['Total\n', 'LY %', 'LY Total'] or\
            pd.isnull(df_fy_store3.iloc[-1]['Store No']):
                raise Exception("ERROR: Structure of the dataframe is not correct")

    # applying the function set below which goes through values and creates new raw data
    new_data = []
    df_fy_store3.apply(lambda z: reorder_func(z, col_names, new_data, fiscal_year), axis=1)
    # new data frame created (from dict works on list of dicts)
    df = pd.DataFrame.from_dict(new_data)
    if save_files:
        df.to_excel(r'test2.xlsx')
    return df


def reorder_func(x, col_names, new_data, fiscal_year):
    """
    utility function for lambda that adds data to new dataframe for each week/store combination
    :param x: df.Series object - 1 row of dataframe
    :param col_names: list(str) - columns of dataframe
    :param new_data: list - empty list that is filled
    :param fiscal_year: str - year that has been read from the dataframe
    """
    # looping values and columns since we need the column name for data in month and week columns
    for y, col_name in zip(x[2:], col_names[2:]):
        new_dict = dict()
        new_dict['Store no'] = x[0]
        new_dict['Store'] = x[1]
        new_dict['Month'] = col_name.split('-')[0]
        new_dict['week_num'] = col_name.split('-')[1]
        new_dict['Year'] = fiscal_year
        new_dict['Sales'] = y
        new_data.append(new_dict)


def update_spacenk(save_files=True, sheets=None, path=""):
    """
    main function that would update the whole Excel file(for this purpose only first two sheets)
    It executes the above functions for each sheet and messages with len of each sheet, so we can log that info for
    Prefect
    :param save_files: bool - True to store the table also locally in Excel to check it
    :param sheets: list(str) - sheets that we want to update default all sheets ['lw_store', 'fy_store']
    :param path: str - path to Excel file that is being read
    :return: list(str) - list of messages
    """

    if sheets is None:
        sheets = ['lw_store', 'fy_store']
    match_string = f'{path}SpaceNK_2.0*.xlsx'
    # Using glob to find the file since I noticed the file name may be inconsistent with (2) (1) in name.
    # I assumed this read from some designated folder and is not in the same folder with other same named files.
    # Also handling error if file is not found, sending it directly to workflow and raising it from there
    updated_table_messages = []
    try:
        file = glob.glob(match_string)[0]
    except IndexError:
        updated_table_messages.append("ERROR: File Space NK was not found")
        return updated_table_messages
    with open(f'{path}config.json', 'r') as fp:
        config_data = json.load(fp)
    engine = create_engine(f'postgresql://{config_data["sql_user"]}:'
                           f'{config_data["sql_password"]}@{config_data["sql_host"]}:'
                           f'{config_data["sql_port"]}/{config_data["sql_name"]}')
    # calling last week store and fiscal year store functions that read the sheets and updates the data
    # Base was imported from other file together with table names below
    spaceNK_base.metadata.create_all(engine)
    updated_table_messages = []

    for sheet in sheets:
        if sheet == 'lw_store':
            try:
                df_lw_store = last_week_store(file, update_closed_stores=True, save_files=save_files)
            except Exception as e:
                updated_table_messages.append(f"ERROR {str(e)}")
                print(str(e))
                continue
            df_lw_store.to_sql(lw_store_table_name, engine, if_exists='replace', index=False)
            updated_table_messages.append(f"Last Week per store : uploaded {df_lw_store.shape[0]} rows")
        elif sheet == 'fy_store':
            try:
                df_fy_store = fiscal_year_store(file, save_files=save_files)
            except Exception as e:
                updated_table_messages.append(f"ERROR {str(e)}")
                print(str(e))
                continue
            df_fy_store.to_sql(fy_store_table_name, engine, if_exists='replace', index=False)
            updated_table_messages.append(f"Fiscal Year  per store : uploaded {df_fy_store.shape[0]} rows")
    return updated_table_messages


if __name__ == '__main__':
    additional_path = "../"
    update_spacenk(save_files=True, path=additional_path)
