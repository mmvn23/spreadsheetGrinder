import copy

from processing.PERSONAL.old.pre_loading import pre_load_objects
from Dataset import Raw, Matrix
import variables.PERSONAL_setup.file as ps_stp
import yfinance as yf


def get_market_cap_from_yfinance(any_ticker_name):
    # Get the ticker information
    ticker = yf.Ticker(any_ticker_name)
    print(any_ticker_name)
    # Get the market capitalization
    market_cap = ticker.info['marketCap']

    return market_cap


def add_market_cap_to_datamatrix(original_datamatrix, market_cap_clmn, ticker_clmn, reset_index=True):
    any_datamatrix = copy.deepcopy(original_datamatrix)

    if reset_index:
        any_datamatrix.remove_index()
    any_datamatrix.dataframe[market_cap_clmn] = any_datamatrix.dataframe.apply(lambda row:
                                                                               get_market_cap_from_yfinance(row[ticker_clmn]),
                                                                               axis=1)
    # add timestamp
    if reset_index:
        any_datamatrix.apply_standard_index()

    return any_datamatrix

# pre_load_objects()
#
market_cap_clmn = 'market_cap'
ticker_clmn = 'ticker'

mtx_stock = Matrix.DataMatrix.load_from_json('stock', root=ps_stp.root_folder, folder=ps_stp.json_folder)
mtx_stock.load_dataframe(any_raw_dataset_name_list=['sp_500'], treat_date=False,
                         root_json=ps_stp.root_folder, folder_json=ps_stp.json_folder)
mtx_stock = add_market_cap_to_datamatrix(original_datamatrix=mtx_stock,
                                         market_cap_clmn=market_cap_clmn, ticker_clmn=ticker_clmn, reset_index=True)
mtx_stock.write(root=ps_stp.root_folder, folder=ps_stp.json_folder, folder_dataframe=ps_stp.dataframe_folder)
# mtx_volume_old = Matrix.DataMatrix.load_old_object('volume', is_for_archive=True)
# print(get_market_cap_from_yfinance("AAPL"))