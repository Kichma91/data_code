from sqlalchemy import MetaData, Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base

lw_store_table_name = 'last_week_report_by_store'
fy_store_table_name = 'fiscal_year_report_by_store'
metadata = MetaData()

spaceNK_base = declarative_base(metadata=metadata)


class LWstore(spaceNK_base):
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
    __tablename__ = fy_store_table_name
    store_no = Column('Store No', String, primary_key=True)
    store = Column('Store', String)
    month = Column('Month', String)
    week_num = Column('Week number', Integer)
    sales = Column('Sales', Float)
