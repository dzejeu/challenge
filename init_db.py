from dateutil.relativedelta import relativedelta

from challenge.eurostat.fetch_and_extract import EurostatDataFetcher
from challenge.eurostat.load_to_db import SqlLiteClient
from multiprocessing import Pool, cpu_count
from datetime import datetime
from dateutil.parser import parse as parse_date_str
import os
from tqdm import tqdm

EXTRACT_DIR = '/home/mswat/extracted/bongo'


def init_sqlite_db(extract_dir,
                   base_url='https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing',
                   filename_template='comext/COMEXT_DATA/PRODUCTS/full%Y%m.7z',
                   db_name='eurostat.db',
                   table_name='trades',
                   start_date='2013-01-01',
                   end_date='2018-05-01'):
    eurostat_client = EurostatDataFetcher(
        base_url=base_url,
        filename_template=filename_template,
        extracted_files_dir=extract_dir,
    )
    proc_pool = Pool(cpu_count())
    (start, end) = map(parse_date_str, [start_date, end_date])
    months_diff = relativedelta(start, end).months
    months_to_fetch = [start + relativedelta(months=month) for month in range(months_diff)]
    # fetch compressed files
    proc_pool.apply_async(eurostat_client.fetch_and_extract_to_file, months_to_fetch)

    extracted_files = [os.path.join(extract_dir, file) for file in os.listdir(extract_dir)]

    for file in tqdm(extracted_files):
        with SqlLiteClient('eurostat.db') as sql:
            try:
                [df] = sql.dataframe_from_csv(file)
                df['VALUE_IN_EUROS'] = df['VALUE_IN_EUROS'].apply(float)
                df = df[['PERIOD', 'DECLARANT_ISO', 'TRADE_TYPE', 'VALUE_IN_EUROS']]
                sql.load_to_db(df, 'trades')
            except MemoryError:
                chunks = sql.dataframe_from_csv(file, True)
                for chunk in chunks:
                    chunk['VALUE_IN_EUROS'] = chunk['VALUE_IN_EUROS'].apply(float)
                    chunk = chunk[['PERIOD', 'DECLARANT_ISO', 'TRADE_TYPE', 'VALUE_IN_EUROS']]
                    sql.load_to_db(chunk, 'trades')
