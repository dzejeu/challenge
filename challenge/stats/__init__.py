from challenge.eurostat.load_to_db import SqlLiteClient
from challenge.config import DB_PATH, TABLE_NAME
from dateutil.relativedelta import relativedelta


def _extend_with_yoy(df):
    df['YoY'] = df['Trades'].pct_change(periods=12)
    return df


def _extend_with_mom(df):
    df['MoM'] = df['Trades'].pct_change(periods=1)
    return df


def _extend_with_moving_avg(df):
    df['MovingAvg'] = df['Trades'].rolling(window=12, min_periods=12).mean()
    return df


def fetch_trades(declarant, trade_type, start_date, end_date):
    start_date -= relativedelta(years=1)
    (start, end) = map(lambda x: x.replace(day=1).strftime('%Y-%m-%d'), [start_date, end_date])
    query = """
        SELECT 
            SUM(VALUE_IN_EUROS) AS Trades,
            PERIOD AS Month
        FROM {table_id}
        WHERE PERIOD BETWEEN "{start_date}" AND "{end_date}"
        AND DECLARANT_ISO = "{declarant}"
        AND TRADE_TYPE = "{trade_type}"
        GROUP BY Month
        ORDER BY Month ASC;
    """

    with SqlLiteClient(DB_PATH) as sql:
        df = sql.query(query.format(table_id=TABLE_NAME,
                                    declarant=declarant,
                                    trade_type=trade_type,
                                    start_date=start,
                                    end_date=end))

    for fn in [_extend_with_mom, _extend_with_yoy, _extend_with_moving_avg]:
        df = fn(df)
    return df


if __name__ == '__main__':
    from datetime import datetime as dt
    df = fetch_trades(declarant='FR', trade_type='I', start_date=dt(2016, 1, 1), end_date=dt(2018, 1, 1))
    df.to_csv('/home/mswat/czele.csv', index=False)
