import pandas as pd
import argparse
from datetime import datetime

from rqalpha.data.data_source import LocalDataSource

import sql.query as qe


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, action='store', required=True, help='start date')
    parser.add_argument('--end_date', type=str, action='store', required=True, help='end date')
    parser.add_argument('--ticker', type=str, action='store', required=False, default=None, help='ticker list')
    parser.add_argument('--bulk_mode', action='store_true', default=False, help='insert one ticker a time')
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, '%Y%m%d').date()
    end_date = datetime.strptime(args.end_date, '%Y%m%d').date()
    ticker_list = []
    if args.ticker:
        ticker_list = args.ticker.split(',')
    data_root_dir = '/home/shawn/.rqalpha'

    if args.bulk_mode:
        populate_price_data_bulk(start_date, end_date, data_root_dir, ticker_list)
    else:
        populate_price_data(start_date, end_date, data_root_dir, ticker_list)

    print("Finished")


def populate_price_data(start_date, end_date, data_root_dir, ticker_list):
    data_source = LocalDataSource(data_root_dir)
    line_map = data_source._daily_table.attrs["line_map"]
    if len(ticker_list) == 0:
        ticker_list = line_map.keys()

    result_df_list = []

    for ticker in ticker_list:
        all_px_data = data_source.get_all_bars(ticker)
        all_px_df = pd.DataFrame(all_px_data)
        all_px_df['date'] = all_px_df['date'].apply(lambda x: datetime.strptime(str(x/1000000), '%Y%m%d').date())
        all_px_df = all_px_df.drop(['open', 'high', 'low', 'volume'], axis=1)
        all_px_df.columns = ['DataDate', 'Price']
        all_px_df = all_px_df[(all_px_df['DataDate'] >= start_date) & (all_px_df['DataDate'] <= end_date)]
        all_px_df['Source'] = 'ricequant'
        all_px_df['Ticker'] = ticker
        result_df_list.append(all_px_df)
    result_df = pd.concat(result_df_list)
    con = qe.get_connection()
    qe.bulk_insert_df(con, result_df, 'PriceDB', 'daily_price')


def populate_price_data_bulk(start_date, end_date, data_root_dir, ticker_list):
    data_source = LocalDataSource(data_root_dir)
    line_map = data_source._daily_table.attrs["line_map"]
    if len(ticker_list) == 0:
        ticker_list = line_map.keys()
    con = qe.get_connection()

    for idx, ticker in enumerate(ticker_list):
        print("Processing {} - {}".format(idx, ticker))
        all_px_data = data_source.get_all_bars(ticker)
        all_px_df = pd.DataFrame(all_px_data)
        all_px_df['date'] = all_px_df['date'].apply(lambda x: datetime.strptime(str(x / 1000000), '%Y%m%d').date())
        all_px_df = all_px_df.drop(['open', 'high', 'low', 'volume'], axis=1)
        all_px_df.columns = ['DataDate', 'Price']
        all_px_df = all_px_df[(all_px_df['DataDate'] >= start_date) & (all_px_df['DataDate'] <= end_date)]
        all_px_df['Source'] = 'ricequant'
        all_px_df['Ticker'] = ticker
        qe.bulk_insert_df(con, all_px_df, 'PriceDB', 'daily_price')


if __name__ == '__main__':
    main()
