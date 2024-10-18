import pandas as pd

def find_arbitrage(instrument_data: pd.DataFrame) -> list:
    orders = []
    instrument_data = instrument_data.sort_values(['bid_px_00'])
    best_bid = instrument_data[['bid_px_00', 'bid_sz_00', 'symbol', 'ts_recv']].reset_index(drop=True)
    instrument_data = instrument_data.sort_values(['ask_px_00'], ascending=False)
    best_ask = instrument_data[['ask_px_00', 'ask_sz_00', 'symbol', 'ts_recv']].reset_index(drop=True)
    
    # concaenate the two dfs horizontally
    concatenated_df = pd.concat([best_bid, best_ask], axis=1)
    bid_pointer = len(concatenated_df) - 1
    ask_pointer = len(concatenated_df) - 1
    
    # display(concatenated_df)
    curr_bid_amt, curr_ask_amt = 0, 0
    while bid_pointer < len(concatenated_df) and ask_pointer >= 0:
        if concatenated_df.at[bid_pointer, 'bid_px_00'] < concatenated_df.at[ask_pointer, 'ask_px_00'] + 0.75:
            break
        else:
            bid_size = concatenated_df.at[bid_pointer, 'bid_sz_00']
            ask_size = concatenated_df.at[ask_pointer, 'ask_sz_00']
            order_size = min(bid_size, ask_size)
            curr_bid, curr_ask = bid_pointer, ask_pointer
            bid_time, ask_time = best_bid.at[curr_bid, 'ts_recv'], best_ask.at[curr_ask, 'ts_recv']
            
            # if ask_time > bid_time:
            #     bid_pointer += 1
            #     continue
            
            pnl = (concatenated_df.at[bid_pointer, 'bid_px_00'] - concatenated_df.at[ask_pointer, 'ask_px_00']) * min(order_size, 100) * 100
                        
            if order_size > 0:
                order_size = min(order_size, 100)
                
                # curr_bid_amt += order_size * concatenated_df.at[bid_pointer, 'bid_px_00']

                # if curr_bid_amt >= 200000:
                #     bid_pointer -= 1
                #     curr_bid_amt = 0
                #     continue
                
                orders.append({
                    "datetime" : best_bid.at[curr_bid, 'ts_recv'],
                    "option_symbol" : best_bid.at[curr_bid, 'symbol'],
                    "action" : "S",
                    "order_size" : order_size,
                    "pnl" : pnl
                })
                orders.append({
                    "datetime" : best_ask.at[curr_ask, 'ts_recv'],
                    "option_symbol" : best_ask.at[curr_ask, 'symbol'],
                    "action" : "B",
                    "order_size" : order_size,
                    "pnl" : pnl
                })

            
            if bid_size > ask_size:
                concatenated_df.at[bid_pointer, 'bid_sz_00'] -= order_size
                ask_pointer -= 1
                curr_ask_amt = 0
            elif bid_size < ask_size:
                concatenated_df.at[ask_pointer, 'ask_sz_00'] -= order_size
                bid_pointer -= 1
                curr_bid_amt = 0
            else:
                concatenated_df.at[bid_pointer, 'bid_sz_00'] -= order_size
                concatenated_df.at[ask_pointer, 'ask_sz_00'] -= order_size
                bid_pointer -= 1
                ask_pointer -= 1
                curr_bid_amt = 0
                curr_ask_amt = 0


                
                
    return orders