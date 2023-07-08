from sqlalchemy import MetaData, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

# these file names will be imported and directly used by other codes
lw_store_table_name = 'last_week_report_by_store'
fy_store_table_name = 'fiscal_year_report_by_store'

# in case we would also implement different Excel files that we want on separate metadata/base
metadata_spacenk = MetaData()
spaceNK_base = declarative_base(metadata=metadata_spacenk)


class LWstore(spaceNK_base):
    """
    Schema for Last week store report
    """
    __tablename__ = lw_store_table_name
    store_no = Column('Store No', String, primary_key=True)
    store = Column('Store', String)
    ty_units = Column('TY Units', Integer)
    ly_units = Column('LY Units', Integer)
    tw_sales = Column('TW Sales', Float)
    lw_sales = Column('LW Sales', Float)
    lw_war_pct = Column('LW Var %', Float)
    ly_sales = Column('LY Sales', Float)
    ly_var_pct = Column('LY Var %', Float)
    ytd_sales = Column('YTD Sales', Float)
    lytd_sales = Column('LYTD Sales', Float)
    lytd_var_pct = Column('LYTD Var %', Float)


class FYstore(spaceNK_base):
    """
    Schema for Fiscal year store report
    """
    __tablename__ = fy_store_table_name
    store_no = Column('Store No', String, primary_key=True)
    store = Column('Store', String)
    month = Column('Month', String)
    week_num = Column('Week number', Integer)
    year = Column('Year', Integer)
    sales = Column('Sales', Float)
