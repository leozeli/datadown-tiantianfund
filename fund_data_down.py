# -*- coding: utf-8 -*-
"""
Created on Mon Dec 28 16:38:19 2020

@author: yeyedas
"""

import json
import queue
import threading
from numpy import size
import requests
import pandas as pd
from sqlalchemy import create_engine
import time
import random
import re
import os
from datetime import datetime
import click
import logging


# 代理


def get_proxy():
    data_json = requests.get("http://127.0.0.1:3000/http").text
    data = json.loads(data_json)
    proxy = 'http://' + data['proxyHost'] + ':' + str(data['proxyPort'])
    return proxy


def build_worth_data(fund_text, fund, date_last):

    pattern = '(?<=Data_netWorthTrend = \[).*(?=\];\/\*累计净值走势\*\/var)'
    unit_temp_index = re.search(pattern, fund_text).span()
    unit_temp = fund_text[unit_temp_index[0]:unit_temp_index[1]]
    unit_dict = eval(unit_temp)
    unit_df = pd.DataFrame(unit_dict)
    unit_df = unit_df[['x', 'y']].rename(
        columns={'x': 'date', 'y': 'netWorth'})
    unit_df.date = (
        unit_df.date * 1000000).astype('datetime64[ns, Asia/Shanghai]')
    unit_df.date = unit_df.date.dt.date.astype('str')
    unit_df['netWorth'] = unit_df['netWorth'].astype('float')
    unit_df['fund'] = fund

    pattern = '(?<=Data_ACWorthTrend = \[).*(?=\]\;\/\*累计收益率走势)'
    ACWorth_temp_index = re.search(pattern, fund_text).span()
    ACWorth_temp = fund_text[ACWorth_temp_index[0]:ACWorth_temp_index[1]]
    ACWorth_temp = ACWorth_temp.replace('null', 'None')
    ACWorth_list = eval(ACWorth_temp)
    ACWorth_df = pd.DataFrame(ACWorth_list, columns=['a', 'ACWorth'])
    ACWorth_df = ACWorth_df[['ACWorth']]

    unit_df = unit_df.join(ACWorth_df)
    unit_df = unit_df.loc[:, ['fund', 'date', 'netWorth', 'ACWorth']]
    unit_df = unit_df[unit_df.date >= date_last]
    if unit_df.ACWorth.isnull().any():
        index_list = unit_df[unit_df.ACWorth.isnull()].index
        for index_t in index_list:
            if index_t == 0:
                unit_df.loc[index_t,
                            'ACWorth'] = unit_df.loc[index_t, 'netWorth']
            else:
                unit_df.loc[index_t, 'ACWorth'] = unit_df.loc[index_t - 1, 'ACWorth'] + (
                    unit_df.loc[index_t, 'netWorth'] - unit_df.loc[index_t - 1, 'netWorth'])
    unit_df['ACWorth'] = unit_df['ACWorth'].astype('float')

    unit_df['growth'] = (
        unit_df['ACWorth'] - unit_df['ACWorth'].shift(1))/unit_df['netWorth'].shift(1)
    unit_df = unit_df[unit_df.date > date_last].reset_index(drop=True)
    if unit_df.loc[0, :].isnull().any():
        unit_df.loc[0, 'growth'] = 0
    unit_df['growth'] = unit_df['growth'].astype('float')
    return unit_df


def get_fund_data(fund_list, engine, name, update, mutex_lock):
    error_time = 0
    user_agent_list = [
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60',
        'Opera/8.0 (Windows NT 5.1; U; en)',
        'Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50',
        'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; en) Opera 9.50',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
        'Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.57.2 (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
        'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)',
        'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)"',
        'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)',
        'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)',
        'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0',
        'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; SE 2.X MetaSr 1.0)',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Maxthon/4.4.3.4000 Chrome/30.0.1599.101 Safari/537.36',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36']

    # referer列表
    referer_list = [
        'https://fund.eastmoney.com/data/fundranking.html',
        'https://fund.eastmoney.com/data/diyfundranking.html',
        'https://fund.eastmoney.com/data/fbsfundranking.html',
        'https://fund.eastmoney.com/data/hbxfundranking.html',
        'https://fund.eastmoney.com/data/lcxfundranking.html',

    ]

    list_error = list()
    start_time = datetime.now()

    header = {'User-Agent': random.choice(user_agent_list),
              'Referer': random.choice(referer_list),
              'Host': 'fund.eastmoney.com'
              }
    fund_code_queue = queue.Queue(len(fund_list.fund))
    # 加入队列
    for  i in range(len(fund_list.fund)):
        fund_code_queue.put(fund_list.fund.iloc[i])

    while(not fund_code_queue.empty()):
        # 从队列读取一个基金代码
        # 读取是阻塞操作
        fund_code = fund_code_queue.get()
        if update:
            date_last = pd.read_sql('SELECT MAX(date) FROM %s_fund_data WHERE fund=\'%s\'' % (
                name, fund_code), engine).iloc[-1, 0]
            if(datetime.strptime(date_last, "%Y-%m-%d").date() >= datetime.today().date()):
                logging.info(f"{name}-无需更新")
                break
        else:
            date_last = '2015-01-01'
        try:
            for i in range(3):
                try:
                    fund_text = requests.get('http://fund.eastmoney.com/pingzhongdata/%s.js' % (fund_code), timeout=3,
                                             headers=header).text
                    break
                except Exception:
                    header = {'User-Agent': random.choice(user_agent_list),
                              'Referer': random.choice(referer_list),
                              'Host': 'fund.eastmoney.com'
                              }
                    fund_text = requests.get('http://fund.eastmoney.com/pingzhongdata/%s.js' % (fund_code), timeout=3,
                                             headers=header, proxies={"http": get_proxy()}).text
                    time.sleep(random.randint(15, 20) / 10)

            pattern = '(?<=测试数据 \* \@type \{arry\} \*//\*).*(?=\*/var ishb\=false)'
            fund_date_new_idx = re.search(pattern, fund_text).span()
            re_date = fund_text[fund_date_new_idx[0]:fund_date_new_idx[1]]
            fund_date_new = pd.to_datetime(re_date).date()
            if (date_last < (fund_date_new - pd.Timedelta(days=1)).strftime("%Y-%m-%d")):
                worth_df = build_worth_data(fund_text, fund_code, date_last)
                if worth_df.empty == False:
                    if ((update == False) & (fund_code == fund_list.fund.iloc[0])):
                        mutex_lock.acquire()
                        worth_df.to_sql(name='%s_fund_data' % (
                            name), con=engine, index=False, if_exists='replace')
                        mutex_lock.release()
                    else:
                        mutex_lock.acquire()
                        worth_df.to_sql(name='%s_fund_data' % (
                            name), con=engine, index=False, if_exists='append')
                        mutex_lock.release()

        except Exception:
            error_time += 1
            if (error_time >= len(fund_list.fund) // 10):
                err_list = []
                while(not fund_code_queue.empty()):
                    err_list.append(fund_code_queue.get())
                    logging.error(f"{name}-错误次数过多|{err_list}")
                    break
            # 访问失败了，所以要把我们刚才取出的数据再放回去队列中
            fund_code_queue.put(fund_code)
            logging.error(f'{name}-{str(fund_code)}该基金下载错误：')
            time.sleep(random.randint(15, 20) / 10)

    end_time = datetime.now()
    used_time = (end_time - start_time).seconds
    logging.info(f'{name}爬取用时：{used_time} 秒')
    return list_error


@click.command()
@click.option('--account', default='root', help='Account of mysql')
@click.option('--password', default='987617162', help='Password of mysql')
@click.option('--host', default='192.168.2.222', help='Host of mysql')
@click.option('--post', default='3306', help='Post of mysql')
@click.option('--database', default='fund', help='database of mysql')
@click.option('--update', default='True', help='Update data or create data')
def main_command(account, password, host, post, database, update):
    # 获取所有基金数据
    if (update == 'True'):
        update = True
    if (update == 'False'):
        update = False
    engine = create_engine('mysql+pymysql://%s:%s@%s:%s/%s' %
                           (account, password, host, post, database))
    # names = ['hybrid', 'bond', 'equity', 'index', 'qdii',
    #          'commodity']  # [混合，债券，股票，指数，QDII，商品（非QDII）]
    names = ['hybrid', 'bond', 'equity', 'index',]
    type_list = [1, 5, 9, 10, 11, 12, 13]
    for i in range(4):

        name = names[i]
        if update:
            fund_list = pd.read_sql('SELECT DISTINCT fund FROM %s_fund_data' % (
                name), engine).sort_values(by='fund')
        else:
            fund_list = pd.read_sql('SELECT DISTINCT fund FROM fund_list WHERE type BETWEEN %d AND %d' % (
                type_list[i], type_list[i + 1] - 1), engine).sort_values(by='fund')

        # print(fund_list.fund.iloc[0])
        # break
        logging.info(f"基金列表-{name}加载完成，爬虫线程启动")
        mutex_lock = threading.Lock()
        # 线程数为50，在一定范围内，线程数越多，速度越快
        for i in range(10):
            t = threading.Thread(target=get_fund_data, name='LoopThread' +
                                str(i), args=(fund_list, engine, name, update, mutex_lock))
            t.start()

        logging.info(f"{name}_fund_data更新完成")

        if update == False:
            with engine.connect() as con:
                con.execute('create index s1 on %s_fund_data(fund(6))' %
                            (name))  # 创建索引
                con.execute(
                    'create index s2 on %s_fund_data(date(12))' % (name))  # 创建索引
                con.execute(
                    'create index s3 on %s_fund_data(fund(6),date(12))' % (name))  # 创建索引
                con.close()
            logging.info(f"{name}索引更新完成")

    return


if __name__ == '__main__':
    task_date = datetime.now().strftime('%H-%M-%S')
    day_date = datetime.now().strftime('%Y-%m-%d')
    if (os.path.exists(f'./logs/{day_date}')):
        pass
    else:
        os.mkdir(f'./logs/{day_date}')
    log_file = open(
        f'./logs/{day_date}/fund_crawler.log', encoding="utf-8", mode="a")
    logging.basicConfig(stream=log_file,
                        level=logging.INFO, format='%(asctime)s %(message)s')
    main_command()
