import random
import pandas as pd
import multiprocessing as mp
from datetime import datetime
from collections import defaultdict


class Strategy:

  def __init__(self, start_date, end_date, options_data, underlying) -> None:
    self.capital: float = 100_000_000
    self.portfolio_value: float = 0
    self.daily_max_orders: int = 150
    self.max_order_size: int = 25

    self.start_date: datetime = start_date
    self.end_date: datetime = end_date

    self.options: pd.DataFrame = pd.read_csv(options_data)
    self.options["day"] = self.options["ts_recv"].apply(
        lambda x: x.split("T")[0])

    # options["day"] = options["ts_recv"].apply(lambda x: x.split("T")[0])
    self.options['day'] = self.options["ts_recv"].apply(lambda x: datetime.strptime(
        x.split("T")[0], "%Y-%m-%d").strftime("%Y-%m-%d"))
    self.options['symbol_data'] = self.options['symbol'].str.split('   ', expand=True)[
        1]
    self.options['expiration_date'] = pd.to_datetime(
        self.options['symbol_data'].str[:6], format="%y%m%d").dt.strftime("%Y-%m-%d")
    self.options['type'] = self.options['symbol_data'].str[6]
    self.options['strike'] = self.options['symbol_data'].str[7:].astype(
        float) / 1000

    self.underlying = pd.read_csv(underlying)
    self.underlying.columns = self.underlying.columns.str.lower()


  def find_arbitrage(self, instrument_data: pd.DataFrame) -> list:

    if instrument_data['expiration_date'].values[0] >= self.end_date.strftime("%Y-%m-%d"):
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
    arbitrage_cutoff = 0.75

    # concaenate the two dfs horizontally
    concatenated_df = pd.concat([best_bid, best_ask], axis=1)
    bid_pointer = len(concatenated_df) - 1
    ask_pointer = len(concatenated_df) - 1

    while bid_pointer < len(concatenated_df) and ask_pointer >= 0:
        idx += 1
        if concatenated_df.at[bid_pointer, 'bid_px_00'] < concatenated_df.at[ask_pointer, 'ask_px_00'] + arbitrage_cutoff:
            break
        else:
            bid_size = concatenated_df.at[bid_pointer, 'bid_sz_00']
            ask_size = concatenated_df.at[ask_pointer, 'ask_sz_00']
            order_size = min(bid_size, ask_size)
            curr_bid, curr_ask = bid_pointer, ask_pointer

            pnl = (concatenated_df.at[bid_pointer, 'bid_px_00'] -
                   concatenated_df.at[ask_pointer, 'ask_px_00']) * min(order_size, self.max_order_size) * 100

            if order_size > 0:
                order_size = min(order_size, self.max_order_size)

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


  def process_instruments(self, args):
    instrument_ids, current_data, orders = args
    for instrument_id in instrument_ids:
      instrument_data = current_data.loc[current_data['instrument_id']
                                        == instrument_id, :]
      res = self.find_arbitrage(instrument_data)
      orders.extend(res)


  def process_date(self, args):
    current_date, options, orders = args

    current_date = current_date.strftime("%Y-%m-%d")

    current_data = options.loc[options['day'] == current_date, :]
    print(current_date, current_data.shape[0])
    if current_data.shape[0] == 0:
        return

    current_data = current_data.sort_values(['instrument_id', 'bid_px_00'])
    instrument_ids = current_data['instrument_id'].unique()

    self.process_instruments((instrument_ids, current_data, orders))


  def postprocess_orders(self, df: pd.DataFrame) -> pd.DataFrame:
    sorted_date_pnl = df.sort_values(
        ['datetime', 'pnl', 'option_symbol'], ascending=[True, False, False])
    sorted_data = sorted_date_pnl.sort_values(
        ['day', 'pnl', 'idx'], ascending=[True, False, True])
    top_100 = sorted_data.groupby('day').head(self.daily_max_orders)
    current_capital = 100_000_000
    idxs = set()
    open_symbols = defaultdict(int)
    df = top_100.sort_values('datetime')

  # df2 = pd.read_csv("data/labeledorders.csv").sort_values(['pnl'], ascending=False).head(30_000)
  # df = df2.sort_values(['datetime', 'pnl', 'option_symbol'], ascending=[True, False, False])

    for row in df.itertuples():
        # print(current_capital)
        if row.Index in idxs:
            continue

        if row.action == 'B':
            options_cost = row.order_size * 100 * row.price
            margin = options_cost + 0.1 * \
                row.strike if row.type == "C" else options_cost + 0.1 * row.price
            margin = 0 if open_symbols[row.option_symbol] > 0 else margin

            if current_capital - (options_cost + .5) > 10_000_000 and current_capital >= margin:
                current_capital -= options_cost + .5
                # If we buy it increase the order size
                open_symbols[row.option_symbol] += row.order_size
            else:
                # stage the option to remove from the list
                print('removing')
                idxs.add(row.Index)
                contra_idx = df.loc[(df['day'] == row.day) & (
                    df['action'] == 'S') & (df['idx'] == row.idx)].index.values[0]
                idxs.add(contra_idx)
        elif row.action == 'S':
            options_cost = row.order_size * 100 * row.price
            margin = options_cost + 0.1 * \
                row.strike if row.type == "C" else options_cost + 0.1 * row.price
            # margin = 0

            if current_capital >= margin:
                current_capital += options_cost
                # if we sell it decrease the order size
                open_symbols[row.option_symbol] -= row.order_size
            else:
                print('removing')
                # stage the option to remove from the list
                idxs.add(row.Index)
                contra_idx = df.loc[(df['day'] == row.day) & (df['action'] == 'B') & (
                    df['idx'] == row.idx), 'idx'].index.values[0]
                idxs.add(contra_idx)

    final_data = df.loc[~df.index.isin(idxs)]
    return final_data


  def generate_orders(self) -> pd.DataFrame:
    current_date = self.start_date

    manager = mp.Manager()
    orders = manager.list()

    dates = pd.date_range(start=current_date, end=self.end_date)

    with mp.Pool(12) as p:
        p.map(self.process_date, [(date, self.options, orders) for date in dates])

    # print(len(orders))
    orders = list(orders)
    df = pd.DataFrame(orders)

    return self.postprocess_orders(df)
