import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(os.path.dirname(os.path.dirname(__file__))+"/collect")
sys.path.append(os.path.dirname(os.path.dirname(__file__))+"/collect/ts")

import datetime
from library.alert import alert
from collect.ts.collecter import tsCollecter

starttime = datetime.datetime.now()
c_ts=tsCollecter()
# c_ts.getAll()
# c_ts.getAStockIndex()

from collect.ts.astockindex import tsAStockIndex
from collect.ts.helper import tsSHelper


from sqlalchemy import create_engine
import pandas as pd
# DB相关
# kzzV7WRcyXHFeVQI
conn = create_engine('mysql+pymysql://root:Rs0jqMZ5XEmcdGoc@localhost/tushare',
                     encoding='utf-8',pool_size=2,max_overflow=2,pool_reset_on_return='commit',
                     echo=False,echo_pool=True, pool_recycle=120, connect_args={'connect_timeout':3600})

def to_table(dataframe,table_name,con=conn, chunksize=2000):
    conn.execution_options(autocommit=True)
    pd.io.sql.to_sql(dataframe,name=table_name,con=con,if_exists='append',index=False,chunksize=chunksize)

def reflect_table(table_name, conn):
    from sqlalchemy import create_engine, MetaData, Table
    from sqlalchemy import inspect

    meta = MetaData()
    user_table = Table(table_name, meta)
    insp = inspect(conn)
    insp.reflect_table(user_table, None)
    return user_table

def read_sql(sql,con=conn):
    if not sql.startswith('/'):
        sql = '/*TDDL:SOCKET_TIMEOUT=900000*/' + sql
    df = pd.read_sql_query(sql=sql,con=con)
    if 'movie_id' in df.columns and not all(df.movie_id.isna()):
        df['movie_id'] = df.movie_id.fillna(0).astype(int)
    if 'show_id' in df.columns:
        df['show_id'] = df.show_id.fillna(0).astype(int)
    return df

if __name__ == '__main__1':
    # tsSHelper.getDataWithLastDate(c_ts.pro,'index_monthly','astock_index_monthly',c_ts.db, ts_code='000001.sh')
    dates = '20220729,20220630,20220531,20220429,20220331,20220228,20220128,20211231,20211130,20211029,20210930,20210831,20210730,20210630,20210531,20210430,20210331,20210226,20210129,20201231,20201130,20201030,20200930,20200831,20200731,20200630,20200529,20200430,20200331,20200228,20200123,20191231,20191129,20191031,20190930,20190830,20190731,20190628,20190531,20190430,20190329,20190228,20190131,20181228,20181130,20181031,20180928,20180831,20180731,20180629,20180531,20180427,20180330,20180228,20180131,20171229,20171130,20171031,20170929,20170831,20170731,20170630,20170531,20170428,20170331,20170228,20170126,20161230,20161130,20161031,20160930,20160831,20160729,20160630,20160531,20160429,20160331,20160229,20160129,20151231,20151130,20151030,20150930,20150831,20150731,20150630,20150529,20150430,20150331,20150227,20150130,20141231,20141128,20141031,20140930,20140829,20140731,20140630,20140530,20140430,20140331,20140228,20140130,20131231,20131129,20131031,20130930,20130830,20130731,20130628,20130531,20130426,20130329,20130228,20130131,20121231,20121130,20121031,20120928,20120831,20120731,20120629,20120531,20120427,20120330,20120229,20120131,20111230,20111130,20111031,20110930,20110831,20110729,20110630,20110531,20110429,20110331,20110228,20110131,20101231,20101130,20101029,20100930,20100831,20100730,20100630,20100531,20100430,20100331,20100226,20100129,20091231,20091130,20091030,20090930,20090831,20090731,20090630,20090527,20090430,20090331,20090227,20090123,20081231,20081128,20081031,20080926,20080829,20080731'
    date_list = sorted(dates.split(','))
    date_list = [i for i in date_list if i[:6] in ['202205','202207'] ]
    for i in date_list:
        print(i)
        tsSHelper.getDataWithArgs(c_ts.pro,'index_monthly','astock_index_monthly',c_ts.db,trade_date=i, ts_code='000001.sh')
        print(i,'ok')



    endtime = datetime.datetime.now()
    print ("------ Custom Tushare数据同步完毕，共耗时:"+str(endtime - starttime)+"------")
    # alert.send('tsCollecter','同步完毕',"Tushare数据同步完毕，共耗时:"+str(endtime - starttime))

if __name__ == '__main__1':
    # tsSHelper.getDataWithArgs(c_ts.pro, 'daily', 'astock_price_daily', c_ts.db, trade_date='20000907')
    # tsSHelper.getDataWithArgs(c_ts.pro, 'daily', 'astock_price_daily', c_ts.db, trade_date='20040211')
    # tsSHelper.getDataWithArgs(c_ts.pro, 'daily', 'astock_price_daily', c_ts.db, trade_date='20170306')
    tsSHelper.getDataWithArgs(c_ts.pro, 'daily', 'astock_price_daily', c_ts.db, trade_date='20190219')

def fix_data(f, t, trade_date=None, ts_code=None, **kwargs):
    tsSHelper.getDataWithArgs(c_ts.pro, f, t, c_ts.db, trade_date=trade_date, ts_code=ts_code, **kwargs)


def fix_finance_all_income():
    from multiprocessing.dummy import Pool
    def func(c):
        try:
            fix_data('income', 'astock_finance_income', ts_code=c, end_date='20150331', report_type='2')
            # 年度
            fix_data('income', 'astock_finance_income', ts_code=c, end_date='20150331', report_type='1', end_type='4')
            print(c)
            # print(i, c)
        except Exception as e:
            print(c, e)


    codes = read_sql('''
            select ts_code from astock_basic 
            where ts_code not in (
              select distinct ts_code from astock_finance_income
              where end_date = '20141231' and report_type = '2' 
            )
            ''').ts_code.tolist()
    print('共需要',len(codes))
    with Pool(5) as pool:
        pool.starmap(func, zip(codes))




if __name__ == '__main__':
    # fix_finance()
    # fix_finance_all_income()
    # fix_data('income', 'astock_finance_income', ts_code='000001.SZ')
    # 单季
    # fix_data('income','astock_finance_income',start_date='20150331',report_type='2')
    # 年度
    # fix_data('income', 'astock_finance_income', start_date='20150331', report_type='1', end_type='4')
    fix_data('daily', 'astock_price_daily', trade_date='20220822')