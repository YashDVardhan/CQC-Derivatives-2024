import multiprocessing as mp
from datetime import datetime
import pandas as pd

end_date = datetime(2024, 3, 30)
start_date = datetime(2024, 1, 1)


def find_arbitrage(instrument_data: pd.DataFrame) -> list:

    if instrument_data['expiration_date'].values[0] >= end_date.strftime("%Y-%m-%d"):
        return []
    elif instrument_data['expiration_date'].values[0] == instrument_data['day'].values[0]:
        return []

    orders = []
    idx = 0
    instrument_data = instrument_data.sort_values(['bid_px_00'])
    best_bid = instrument_data[['bid_px_00', 'bid_sz_00', 'symbol', 'ts_recv',
                                'day', 'type', 'expiration_date', 'strike']].reset_index(drop=True)
    instrument_data = instrument_data.sort_values(
        ['ask_px_00'], ascending=False)
    best_ask = instrument_data[['ask_px_00', 'ask_sz_00', 'symbol', 'ts_recv',
                                'day', 'type', 'expiration_date', 'strike']].reset_index(drop=True)

    # concaenate the two dfs horizontally
    concatenated_df = pd.concat([best_bid, best_ask], axis=1)
    bid_pointer = len(concatenated_df) - 1
    ask_pointer = len(concatenated_df) - 1

    while bid_pointer < len(concatenated_df) and ask_pointer >= 0:
        idx += 1
        if concatenated_df.at[bid_pointer, 'bid_px_00'] < concatenated_df.at[ask_pointer, 'ask_px_00'] + 0.75:
            break
        else:
            bid_size = concatenated_df.at[bid_pointer, 'bid_sz_00']
            ask_size = concatenated_df.at[ask_pointer, 'ask_sz_00']
            order_size = min(bid_size, ask_size)
            curr_bid, curr_ask = bid_pointer, ask_pointer

            pnl = (concatenated_df.at[bid_pointer, 'bid_px_00'] -
                   concatenated_df.at[ask_pointer, 'ask_px_00']) * min(order_size, 50) * 100

            if order_size > 0:
                order_size = min(order_size, 50)

                orders.append({
                    "datetime": best_bid.at[curr_bid, 'ts_recv'],
                    "option_symbol": best_bid.at[curr_bid, 'symbol'],
                    "action": "S",
                    "order_size": order_size,
                    "price": concatenated_df.at[bid_pointer, 'bid_px_00'],
                    "pnl": pnl,
                    "day": best_bid.at[curr_bid, 'day'],
                    "type": best_bid.at[curr_bid, 'type'],
                    'strike': best_bid.at[curr_bid, 'strike'],
                    'idx': idx,
                    'expiration_date': best_bid.at[curr_bid, 'expiration_date']
                })
                orders.append({
                    "datetime": best_ask.at[curr_ask, 'ts_recv'],
                    "option_symbol": best_ask.at[curr_ask, 'symbol'],
                    "action": "B",
                    "order_size": order_size,
                    "price": concatenated_df.at[ask_pointer, 'ask_px_00'],
                    "pnl": pnl,
                    "day": best_ask.at[curr_ask, 'day'],
                    "type": best_ask.at[curr_ask, 'type'],
                    'strike': best_ask.at[curr_ask, 'strike'],
                    'idx': idx,
                    'expiration_date': best_ask.at[curr_ask, 'expiration_date']
                })

            if bid_size > ask_size:
                concatenated_df.at[bid_pointer, 'bid_sz_00'] -= order_size
                ask_pointer -= 1
            elif bid_size < ask_size:
                concatenated_df.at[ask_pointer, 'ask_sz_00'] -= order_size
                bid_pointer -= 1
            else:
                concatenated_df.at[bid_pointer, 'bid_sz_00'] -= order_size
                concatenated_df.at[ask_pointer, 'ask_sz_00'] -= order_size
                bid_pointer -= 1
                ask_pointer -= 1

    return orders


def process_instruments(args):
    instrument_ids, current_data, orders = args
    for instrument_id in instrument_ids:
        instrument_data = current_data.loc[current_data['instrument_id']
                                           == instrument_id, :]
        res = find_arbitrage(instrument_data)
        orders.extend(res)


def process_date(args):
    current_date, options, orders = args

    current_date = current_date.strftime("%Y-%m-%d")

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

    # options["day"] = options["ts_recv"].apply(lambda x: x.split("T")[0])
    options['day'] = options["ts_recv"].apply(lambda x: datetime.strptime(
        x.split("T")[0], "%Y-%m-%d").strftime("%Y-%m-%d"))
    options['symbol_data'] = options['symbol'].str.split('   ', expand=True)[1]
    options['expiration_date'] = pd.to_datetime(
        options['symbol_data'].str[:6], format="%y%m%d").dt.strftime("%Y-%m-%d")
    options['type'] = options['symbol_data'].str[6]
    options['strike'] = options['symbol_data'].str[7:].astype(float) / 1000

    dates = pd.date_range(start=current_date, end=end_date)

    with mp.Pool(12) as p:
        p.map(process_date, [(date, options, orders) for date in dates])

    print(len(orders))
    orders = list(orders)
    df = pd.DataFrame(orders)
    sorted_df = df.sort_values(['pnl', 'option_symbol'], ascending=False)
    sorted_df.to_csv("data/allorders.csv", index=False)
