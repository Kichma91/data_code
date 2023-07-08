import json
import glob

import pandas as pd
from sqlalchemy import create_engine

from base_files.space_nk_base_files import spaceNK_base, lw_store_table_name, fy_store_table_name

"""
if the task was to read all the tables from all the sheets, I would've separated all of these functions
to another file. Besides reading first sheet, I also added code for 2nd sheet. Also, looking at the other
tables I think the code would be mostly a variation of these 2 codes below.
The code was tested on my PC and succesfully uploaded the data to PostGreSQL server
"""


"""function that reads first sheet and updates it"""
def last_week_store(file):
    # reading excel, removing empty cols and rows, filling nan values with 0 and removing the subtotals
    df_lw_store = pd.read_excel(file, sheet_name="Last Week Report by Store",
                                skiprows=5).drop(columns=['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 4']).fillna(0)[:-1]
    # in case we want to remove closed stores
    df_lw_store = df_lw_store.loc[~df_lw_store['Store'].str.contains('CLOSED')].reset_index(drop=True)
    df_lw_store.to_excel(r'test1.xlsx')
    return df_lw_store


'''
Idea with this function is that it converts the pivoted table from sheet 2 into raw data and stores it
'''
def fiscal_year_store(file):
    # removing Nan columns while loading, and the first total(YTD) column
    df_fy_store = pd.read_excel(file, sheet_name="Fiscal Year Report by Store", skiprows=2).drop(
        columns=['Unnamed: 0', 'Unnamed: 1', 'Unnamed: 4', 'Unnamed: 5'])
    # this piece of code finds the index of the next table that is containing last year data
    last_year_index = df_fy_store.loc[df_fy_store['Unnamed: 2'].fillna('null').str.contains('Year')].iloc[1].name
    # this filters out just the current year(hard coded supposing structure won't change)
    df_fy_store2 = df_fy_store[:last_year_index - 5].reset_index(drop=True)
    # This is separated last year table in case we would need it. In that case I would store the below
    # part in a separate function so I could call it on both dataframes easily
    df_fy_store_VJ = df_fy_store[last_year_index:-1].reset_index(drop=True)
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
    # applying the function set below which goes through values and creates new raw data
    new_data = []
    df_fy_store3.apply(lambda z: reorder_func(z, col_names, new_data, fiscal_year), axis=1)
    # new data frame created (from dict works on list of dicts)
    df = pd.DataFrame.from_dict(new_data)
    df.to_excel(r'test2.xlsx')
    return df


def reorder_func(x, col_names, new_data, fiscal_year):
    for y, col_name in zip(x[2:], col_names[2:]):
        new_dict = dict()
        new_dict['Store no'] = x[0]
        new_dict['Store'] = x[1]
        new_dict['Month'] = col_name.split('-')[0]
        new_dict['week_num'] = col_name.split('-')[1]
        new_dict['Year'] = fiscal_year
        new_dict['Sales'] = y
        new_data.append(new_dict)


def update_spaceNK():
    # Using glob to find the file since I noticed the file name may be incosistent with (2) (1) in name.
    # I assumed this file is imported somewhere and is not in the same folder with other same named files.
    match_string = "SpaceNK_2.0*.xlsx"
    file = glob.glob(match_string)[0]
    with open('login_data.json', 'r') as fp:
        login_data = json.load(fp)
    engine = create_engine(f'postgresql://{login_data["sql_user"]}:'
                           f'{login_data["sql_password"]}@{login_data["sql_host"]}:'
                           f'{login_data["sql_port"]}/{login_data["sql_name"]}')
    # calling last week store function that reads the first sheet and updates the data
    df_lw_store = last_week_store(file)
    df_fy_store = fiscal_year_store(file)
    # Base was imported from other file together with table names below
    spaceNK_base.metadata.create_all(engine)
    df_lw_store.to_sql(lw_store_table_name, engine, if_exists='replace', index=False)
    df_fy_store.to_sql(fy_store_table_name, engine, if_exists='replace', index=False)
    print("Finished uploading SpaceNK file")
    return df_lw_store.shape[0], df_fy_store.shape[0]

if __name__ == '__main__':
    update_spaceNK()


'''in case you want to store login info json without too much hassle if youre testing this code'''
# login_data = {
# 'sql_host':'localhost',
# 'sql_port' : '5432',
# 'sql_name' : 'postgres',
# 'sql_user' : 'postgres',
# 'sql_password' : 'mypassword'}
#
# json_object = json.dumps(login_data,indent=4)
# with open('login_data.json', 'w') as fp:
#     fp.write(json_object)
