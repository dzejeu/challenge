import os
from multiprocessing import Pool, cpu_count

from dateutil.parser import parse as parse_date_str
from dateutil.relativedelta import relativedelta
from tqdm import tqdm
from datetime import datetime

from challenge.eurostat.fetch_and_extract import EurostatDataFetcher, InvalidEurostatApiResponse
from challenge.eurostat.load_to_db import SqlLiteClient
from argparse import ArgumentParser
import logging

log = logging.getLogger(__name__)


def _try_to_fetch(client, date):
    try:
        client.fetch_and_extract_to_file(date)
    except InvalidEurostatApiResponse:
        log.warning('Failed to fetch data for %s', date)


def _aggregate_df_chunk(chunk):
    categorical_columns = ['DECLARANT_ISO', 'TRADE_TYPE', 'PERIOD']
    agg_chunk = (
        chunk
        .set_index(categorical_columns)
        .groupby(categorical_columns)
        .apply(sum)
        .reset_index()
    )
    return agg_chunk


def init_sqlite_db(extract_dir,
                   base_url,
                   filename_template,
                   db_name,
                   table_name,
                   start_date,
                   end_date,
                   clear_dir):
    """
    Fetches compressed data from Eurostat API, extracts it into files and loads into SQLite db removing previously
    created files.
    """
    eurostat_client = EurostatDataFetcher(
        base_url=base_url,
        filename_template=filename_template,
        extracted_files_dir=extract_dir,
    )
    (start, end) = map(parse_date_str, [start_date, end_date])
    relative_diff = relativedelta(end, start)
    months_diff = relative_diff.months + relative_diff.years * 12
    months_to_fetch = [start + relativedelta(months=month) for month in range(months_diff)]

    proc_pool = Pool(cpu_count())
    jobs_hooks = [proc_pool.apply_async(_try_to_fetch, (eurostat_client, month)) for month in months_to_fetch]
    proc_pool.close()
    for hook in jobs_hooks:
        hook.get()

    extracted_files = [os.path.join(extract_dir, file) for file in os.listdir(extract_dir)]
    with SqlLiteClient(db_name) as sql:
        for file in tqdm(extracted_files):
                chunks = sql.dataframe_from_csv(file, True)
                for chunk in chunks:
                    chunk['VALUE_IN_EUROS'] = chunk['VALUE_IN_EUROS'].apply(float)
                    chunk['PERIOD'] = chunk['PERIOD'].apply(lambda x: datetime.strptime(str(x), '%Y%m'))
                    chunk = chunk[['PERIOD', 'DECLARANT_ISO', 'TRADE_TYPE', 'VALUE_IN_EUROS']]
                    agg_chunk = _aggregate_df_chunk(chunk)
                    sql.load_to_db(agg_chunk, table_name)
        sql.create_index(table_name, 'idx', ['DECLARANT_ISO', 'TRADE_TYPE', 'VALUE_IN_EUROS', 'PERIOD'])

    if clear_dir:
        for file in extracted_files:
            os.remove(file)


if __name__ == '__main__':
    parser = ArgumentParser(description=init_sqlite_db.__doc__)
    parser.add_argument('--extract-dir', type=str, help='Where should uncompressed files be stored.'
                                                        'If does not exists it will be crated.')
    parser.add_argument('--base-url',
                        default='https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing',
                        type=str,
                        help='Url path for api requests.')
    parser.add_argument('--filename-template',
                        default='comext/COMEXT_DATA/PRODUCTS/full%Y%m.7z',
                        type=str,
                        help='Path to the file in Eurostat file tree with date format injected. '
                             'Example: some_dir/some_sub_dir/some_file_%%Y%%m.7z')
    parser.add_argument('--db-name',
                        default='eurostat.db',
                        type=str,
                        help='Name of database, if does not exists it will be created.')
    parser.add_argument('--table-name',
                        default='trades',
                        type=str,
                        help='Name of table, if does not exists it will be created.')
    parser.add_argument('--start-date',
                        default='2014-01-01',
                        type=str,
                        help='From which month fetching should be started.')
    parser.add_argument('--end-date',
                        default='2018-05-01',
                        type=str,
                        help='When data fetching should be stopped.')
    parser.add_argument('--clear-dir',
                        default=True,
                        type=bool,
                        help='If set to True all extracted files will be deleted after loading to db.')

    fn_args = vars(parser.parse_args())
    init_sqlite_db(**fn_args)
