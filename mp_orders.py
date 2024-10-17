import multiprocessing as mp
from datetime import datetime
import pandas as pd
from arbitrage import find_arbitrage 

def process_instruments(args):
    instrument_ids, current_data, orders = args
    for instrument_id in instrument_ids:
        instrument_data = current_data.loc[current_data['instrument_id'] == instrument_id, :]
        res = find_arbitrage(instrument_data)
        orders.extend(res)

def process_date(args):
    current_date, options, orders = args
    current_data = options.loc[options['day'] == current_date, :]
    print(current_date, current_data.shape[0])
    if current_data.shape[0] == 0:
        return

    current_data = current_data.sort_values(['instrument_id', 'bid_px_00'])
    instrument_ids = current_data['instrument_id'].unique()

    process_instruments((instrument_ids, current_data, orders))

if __name__ == '__main__':
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 3, 30)

    current_date = start_date
    arbitrage_cutoff = 0.75

    manager = mp.Manager()
    orders = manager.list()

    options = pd.read_csv("data/cleaned_options_data.csv")
    options["day"] = options["ts_recv"].apply(lambda x: x.split("T")[0])
    options['day'] = options['day'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))

    dates = pd.date_range(start=current_date, end=end_date)

    with mp.Pool(12) as p:
        p.map(process_date, [(date, options, orders) for date in dates])

    print(len(orders))
    orders = list(orders)
    df = pd.DataFrame(orders)
    sorted_df = df.sort_values(['pnl', 'option_symbol'], ascending=False).head(1000)
    sorted_df.to_csv("data/labeledorders.csv", index=False)