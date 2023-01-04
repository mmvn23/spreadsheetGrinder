import pandas as pd
import numpy as np
import datetime
from old import functions as fc

# List of inputs:
#   1) Lagged (USA Apr-19 to Jul-20): 1 excel file once             OK
#   2) Lagged (BRA Sep-13 to Mar-19): 1 excel file once
#   3) Lagged (BRA Sep-12 to Aug-13): 12 excel files once
#   4) Lagged (EUR Sep-10 to Aug-12: 24 excel files once
#   5) Chase Account: CSV ongoing (checking, savings, credit card)  OK
#   7) Capital One:  CSV ongoing (checking, savings)                OK
#   8) Pinnacle: TBD ongoing (checking, savings)                    OK
#   9) Nuconta: CSV, OFX ongoing (checking, credit card)            OK
#   10) Fidelity: CSV and XLSX ongoing (401K and HSA)               OK
#   12) Pay stubs: XLSX ongoing                                     OK
#   13) Easynvest: XLSX ongoing                                     OK
#   14) LCL: TBD ongoing
#   15) Amazon: TBD ongoing                                         OK
#   16) Budgeting: TBD ongoing

# Steps:
#   1) Data Cleaning
#       a) Load files
#       b) Check column integrity
#       c) Drop extra columns
#       d) Drop extra rows
#       e) Change column names
#       f) Match raw file to df_cashmov
#       g) Append df_cashmov
#       h) Categorize
#       i) Adjust data types
#   2) Cash flow forecasting
#       a) Load budgeting files
#       b) Check column integrity
#       c) Drop extra columns
#       d) Drop extra rows
#       e) Change column names
#       f) forecasting
#       g) input credit card files
#       h) Append df_cashmov
#   3) Budgeting
#       a) Prepare df_bgt
#       b) Calculate budget positions based on df_cashmov
#   4) Liquidity
#       a) Agregate accounts positions
#       b) Estimate requirements (short, mid, long and retirement)

#############################################    STEP 1) DATA CLEANING    #############################################
##### INPUTS #####
is_first_time = True
error_msg = ''
initial_date_str = '07-01-2020' # mm-dd-yyyy
initial_date = pd.Timestamp(initial_date_str)

origin_clmn = 'File'
description_clmn = 'Description'
value_original_clmn = 'Original value'
currency_clmn = 'Currency'
date_clmn = 'Date'
bgt_l0_clmn = 'Budget 0'
balance_clmn = 'Balance'
debit_clmn = 'Debit'
credit_clmn = 'Credit'
index_clmn = 'Index'
manual_bgt_clmn = 'Manual bgt'
contain_clmn = 'Contain'
auto_bgt_clmn = 'Auto bgt'
initial_date_clmn = 'Initial date'
end_date_clmn = 'End date'

flnm_cashmov_csv = './outputs/Cash Movement Report.csv'
flnm_cashmov_not_assigned_csv = './outputs/Pending items.csv'
flnm_template = './inputs/TEMPLATE.xlsx'

#### File 1 - Lagged (USA Apr-19 to Jul-20) ####
if is_first_time:
    initial_date_start_str = '01-01-2000'  # mm-dd-yyyy
    initial_date_start = pd.Timestamp(initial_date_start_str)
    flnm_lag_USA20 = './inputs/PnL USA 2.xlsx'
    sht_lag_USA20_losses = 'Losses'
    sht_lag_USA20_profit = 'Profit'
    df_lag_USA20_losses_raw_clmn_expect_lt = ['Description', 'Value', 'Currency', 'Date']
    df_lag_USA20_profit_raw_clmn_expect_lt = ['Description', 'Value', 'Currency', 'Date']
    lag_USA20_losses_str = 'Lag USA20 - Losses'
    lag_USA20_profit_str = 'Lag USA20 - Profit'
    type_lag_USA20_profit = 'xlsx'
    type_lag_USA20_losses = 'xlsx'
    lag_USA20_currency = 'N/A'
    lag_USA20_losses_multiplier = -1
    lag_USA20_profit_multiplier = 1

#### File 5 - Chase ####
flnm_chase_savings = './inputs/Chase3179_Activity_20201026.csv'
flnm_chase_checking = './inputs/Chase6026_Activity_20201026.csv'
flnm_chase_credit = './inputs/Chase7329_Activity20200101_20201031_20201031.csv'
df_chase_savings_raw_clmn_expect_lt = ['Description', 'Amount', 'Posting Date', 'Balance']
df_chase_checking_raw_clmn_expect_lt = ['Description', 'Amount', 'Posting Date', 'Balance']
df_chase_credit_raw_clmn_expect_lt = ['Description', 'Amount', 'Post Date']
chase_multiplier_savings = 1
chase_multiplier_checking = 1
chase_multiplier_credit = 1
type_chase_savings = 'csv'
type_chase_checking = 'csv'
type_chase_credit = 'csv'
chase_currency = 'USD'
chase_savings_str = 'Chase Savings'
chase_checking_str = 'Chase Checking'
chase_credit_str = 'Chase Credit'

#### File 7 - Capital One ####
flnm_cptlone_savings = './inputs/2020-10-28_transaction_savings.csv'
flnm_cptlone_checking = './inputs/2020-10-28_transaction_checking.csv'
df_cptlone_savings_raw_clmn_expect_lt = ['Transaction Date', 'Transaction Amount', 'Transaction Description', 'Balance']
df_cptlone_checking_raw_clmn_expect_lt = ['Transaction Date', 'Transaction Amount', 'Transaction Description',
                                          'Balance']
cptlone_currency = 'USD'
cptlone_savings_str = 'Capital One Savings'
cptlone_checking_str = 'Capital One Checking'
cptlone_multiplier = 1
type_cptlone_savings = 'csv'
type_cptlone_checking = 'csv'

#### File 8 - Pinnacle ####
flnm_pnncl_savings = './inputs/Transactions-295-2020-10-31.csv'
flnm_pnncl_checking = './inputs/Transactions-930-2020-10-31.csv'
df_pnncl_savings_raw_clmn_expect_lt = ['Debit', 'Credit', 'Balance', 'Date', 'Description']
df_pnncl_checking_raw_clmn_expect_lt = ['Debit', 'Credit', 'Balance', 'Date', 'Description']
pnncl_currency = 'USD'
pnncl_savings_str = 'Pinnacle Savings'
pnncl_checking_str = 'Pinnacle Checking'
pnncl_multiplier = 1
type_pnncl_savings = 'csv pnncl'
type_pnncl_checking = 'csv pnncl'

#### File 9 - Nubank ####
flnm_nbnk_credit = './inputs/nubank-2020-10.csv'
flnm_nbnk_checking = flnm_template
sht_nbnk_checking = 'Nuconta'
df_nbnk_credit_raw_clmn_expect_lt = ['date', 'title', 'amount']
df_nbnk_checking_raw_clmn_expect_lt = ['Nome', 'Valor', 'Data', 'Saldo depois da despesa']
nbnk_credit_str = 'Nubank credit card'
nbnk_checking_str = 'Nubank checking'
nbnk_currency = 'BRL'
nbnk_credit_multiplier = -1
nbnk_checking_multiplier = 1
type_nbnk_credit = 'csv'
type_nbnk_checking = 'xlsx'

#### File 10 - Fidelity ####
flnm_fdlt_401k = './inputs/history.csv'
df_fdlt_401k_raw_clmn_expect_lt = ['Date', 'Transaction Type', 'Amount']
fdlt_401k_skip_rows = 4
fdlt_401k_str = 'Fidelity 401K'
type_fdlt_401k = 'csv skiprows'
fdlt_currency = 'USD'
fdlt_multiplier_checking = 1

#### File 12 - Payment stubs ####
flnm_pmntstbs = flnm_template
sht_pmntstbs = 'Payment stubs'
df_pmntstbs_raw_clmn_expect_lt = ['Date', 'Description', 'Amount']
pmntstbs_str = 'Payment Stubs'
pmntstbs_currency = 'USD'
pmntstbs_multiplier = 1
type_pmntstbs = 'xlsx'

#### File 13 - Easynvest ####
flnm_snvst_checking = './inputs/Extrato_2020-09-28.csv'
df_snvst_checking_raw_clmn_expect_lt = ['Movim.', 'Histórico', 'Lançamento', 'Saldo']
snvst_checking_str = 'Easynvest Checking'
snvst_currency = 'BRL'
snvst_multiplier = 1
type_snvst_checking = 'csv snvst'

#### File 15 - Amazon ####
flnm_mzn = flnm_template
sht_mzn = 'Amazon'
df_mzn_raw_clmn_expect_lt = ['Order data', 'Item description', 'Adj Item cost']
mzn_str = 'Amazon'
mzn_currency = 'USD'
mzn_multiplier = -1
type_mzn = 'xlsx'

#### File 16 - Budgeting ####
flnm_manual_bgt = flnm_template
sht_manual_bgt = 'Manual Budgets'
df_manual_bgt_raw_clmn_expect_lt = ['Index', bgt_l0_clmn]

flnm_auto_bgt = flnm_template
sht_auto_bgt = "Budgeting contains"
df_auto_bgt_raw_clmn_expect_lt = ['Contains', 'Bgt 0', 'File', 'Initial date', 'End date']

########## File processing ##########

df_cashmov_clmn_lt = [origin_clmn, date_clmn, description_clmn, value_original_clmn, currency_clmn, bgt_l0_clmn]
df_cashmov = pd.DataFrame(columns=df_cashmov_clmn_lt)

#### File 1 - Lagged (USA Apr-19 to Jul-20) ####
if is_first_time:
    lag_USA20_losses_clmn_conversion = {df_lag_USA20_losses_raw_clmn_expect_lt[0]: description_clmn,
                                        df_lag_USA20_losses_raw_clmn_expect_lt[1]: value_original_clmn,
                                        df_lag_USA20_losses_raw_clmn_expect_lt[2]: currency_clmn,
                                        df_lag_USA20_losses_raw_clmn_expect_lt[3]: date_clmn}
    df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_lag_USA20, df_lag_USA20_losses_raw_clmn_expect_lt,
                                lag_USA20_losses_clmn_conversion, description_clmn, date_clmn, value_original_clmn,
                                currency_clmn, origin_clmn, lag_USA20_losses_str, lag_USA20_currency,
                                lag_USA20_losses_multiplier, initial_date_start, type_lag_USA20_losses,
                                sht_lag_USA20_losses)

    lag_USA20_profit_clmn_conversion = {df_lag_USA20_profit_raw_clmn_expect_lt[0]: description_clmn,
                                        df_lag_USA20_profit_raw_clmn_expect_lt[1]: value_original_clmn,
                                        df_lag_USA20_profit_raw_clmn_expect_lt[2]: currency_clmn,
                                        df_lag_USA20_profit_raw_clmn_expect_lt[3]: date_clmn}
    df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_lag_USA20, df_lag_USA20_profit_raw_clmn_expect_lt,
                                lag_USA20_profit_clmn_conversion, description_clmn, date_clmn, value_original_clmn,
                                currency_clmn, origin_clmn, lag_USA20_profit_str, lag_USA20_currency,
                                lag_USA20_profit_multiplier, initial_date_start, type_lag_USA20_profit,
                                sht_lag_USA20_profit)

#### File 5 - Chase ####
chase_savings_clmn_conversion = {df_chase_savings_raw_clmn_expect_lt[0]: description_clmn,
                                 df_chase_savings_raw_clmn_expect_lt[1]: value_original_clmn,
                                 df_chase_savings_raw_clmn_expect_lt[2]: date_clmn,
                                 df_chase_savings_raw_clmn_expect_lt[3]: balance_clmn}
df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_chase_savings, df_chase_savings_raw_clmn_expect_lt,
                            chase_savings_clmn_conversion, description_clmn, date_clmn, value_original_clmn,
                            currency_clmn, origin_clmn, chase_savings_str, chase_currency, chase_multiplier_savings,
                            initial_date, type_chase_savings)

chase_checking_clmn_conversion = chase_savings_clmn_conversion
df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_chase_checking, df_chase_checking_raw_clmn_expect_lt,
                            chase_checking_clmn_conversion, description_clmn, date_clmn, value_original_clmn,
                            currency_clmn, origin_clmn, chase_checking_str, chase_currency, chase_multiplier_checking,
                            initial_date, type_chase_checking)

chase_credit_clmn_conversion = {df_chase_credit_raw_clmn_expect_lt[0]: description_clmn,
                                df_chase_credit_raw_clmn_expect_lt[1]: value_original_clmn,
                                df_chase_credit_raw_clmn_expect_lt[2]: date_clmn}
df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_chase_credit, df_chase_credit_raw_clmn_expect_lt,
                            chase_credit_clmn_conversion, description_clmn, date_clmn, value_original_clmn,
                            currency_clmn, origin_clmn, chase_credit_str, chase_currency, chase_multiplier_credit,
                            initial_date, type_chase_credit)

#### File 7 - Capital One ####
cptlone_savings_clmn_conversion = {df_cptlone_savings_raw_clmn_expect_lt[0]: date_clmn,
                                   df_cptlone_savings_raw_clmn_expect_lt[1]: value_original_clmn,
                                   df_cptlone_savings_raw_clmn_expect_lt[2]: description_clmn,
                                   df_cptlone_savings_raw_clmn_expect_lt[3]: balance_clmn}
df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_cptlone_savings, df_cptlone_savings_raw_clmn_expect_lt,
                            cptlone_savings_clmn_conversion, description_clmn, date_clmn, value_original_clmn,
                            currency_clmn, origin_clmn, cptlone_savings_str, cptlone_currency, cptlone_multiplier,
                            initial_date, type_cptlone_savings)

cptlone_checking_clmn_conversion = cptlone_savings_clmn_conversion
df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_cptlone_checking,
                            df_cptlone_checking_raw_clmn_expect_lt, cptlone_checking_clmn_conversion, description_clmn,
                            date_clmn, value_original_clmn, currency_clmn, origin_clmn, cptlone_checking_str,
                            cptlone_currency, cptlone_multiplier, initial_date, type_cptlone_checking)

#### File 8 - Pinnacle ####
pnncl_savings_clmn_conversion = {df_pnncl_savings_raw_clmn_expect_lt[0]: debit_clmn,
                                 df_pnncl_savings_raw_clmn_expect_lt[1]: credit_clmn,
                                 df_pnncl_savings_raw_clmn_expect_lt[2]: balance_clmn,
                                 df_pnncl_savings_raw_clmn_expect_lt[3]: date_clmn,
                                 df_pnncl_savings_raw_clmn_expect_lt[4]: description_clmn}
df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_pnncl_savings,
                            df_pnncl_savings_raw_clmn_expect_lt, pnncl_savings_clmn_conversion, description_clmn,
                            date_clmn, value_original_clmn, currency_clmn, origin_clmn, pnncl_savings_str,
                            pnncl_currency, pnncl_multiplier, initial_date, type_pnncl_savings,
                            credit_clmn=credit_clmn, debit_clmn=debit_clmn)

pnncl_checking_clmn_conversion = pnncl_savings_clmn_conversion
df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_pnncl_checking,
                            df_pnncl_checking_raw_clmn_expect_lt, pnncl_checking_clmn_conversion, description_clmn,
                            date_clmn, value_original_clmn, currency_clmn, origin_clmn, pnncl_checking_str,
                            pnncl_currency, pnncl_multiplier, initial_date, type_pnncl_checking,
                            credit_clmn=credit_clmn, debit_clmn=debit_clmn)
#### File 9 - Nubank ####
nbnk_credit_clmn_conversion = {df_nbnk_credit_raw_clmn_expect_lt[0]: date_clmn,
                               df_nbnk_credit_raw_clmn_expect_lt[1]: description_clmn,
                               df_nbnk_credit_raw_clmn_expect_lt[2]: value_original_clmn}
df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_nbnk_credit,
                            df_nbnk_credit_raw_clmn_expect_lt, nbnk_credit_clmn_conversion, description_clmn,
                            date_clmn, value_original_clmn, currency_clmn, origin_clmn, nbnk_credit_str,
                            nbnk_currency, nbnk_credit_multiplier, initial_date, type_nbnk_credit)

nbnk_checking_clmn_conversion = {df_nbnk_checking_raw_clmn_expect_lt[0]: description_clmn,
                                 df_nbnk_checking_raw_clmn_expect_lt[1]: value_original_clmn,
                                 df_nbnk_checking_raw_clmn_expect_lt[2]: date_clmn,
                                 df_nbnk_checking_raw_clmn_expect_lt[3]: balance_clmn}
df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_nbnk_checking,
                            df_nbnk_checking_raw_clmn_expect_lt, nbnk_checking_clmn_conversion, description_clmn,
                            date_clmn, value_original_clmn, currency_clmn, origin_clmn, nbnk_checking_str,
                            nbnk_currency, nbnk_checking_multiplier, initial_date, type_nbnk_checking, sht_nbnk_checking)

#### File 10 - Fidelity ####
fdlt_401k_clmn_conversion = {df_fdlt_401k_raw_clmn_expect_lt[0]: date_clmn,
                             df_fdlt_401k_raw_clmn_expect_lt[1]: description_clmn,
                             df_fdlt_401k_raw_clmn_expect_lt[2]: value_original_clmn}
df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_fdlt_401k,
                            df_fdlt_401k_raw_clmn_expect_lt, fdlt_401k_clmn_conversion, description_clmn,
                            date_clmn, value_original_clmn, currency_clmn, origin_clmn, fdlt_401k_str,
                            fdlt_currency, fdlt_multiplier_checking, initial_date, type_fdlt_401k,
                            local_file_skiprows=fdlt_401k_skip_rows)

# Fidelity HSA will be implemented in the future if necessary.

#### File 12 - Payment stubs ####
pmntstbs_clmn_conversion = {df_pmntstbs_raw_clmn_expect_lt[0]: date_clmn,
                            df_pmntstbs_raw_clmn_expect_lt[1]: description_clmn,
                            df_pmntstbs_raw_clmn_expect_lt[2]: value_original_clmn}
df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_pmntstbs,
                            df_pmntstbs_raw_clmn_expect_lt, pmntstbs_clmn_conversion, description_clmn,
                            date_clmn, value_original_clmn, currency_clmn, origin_clmn, pmntstbs_str,
                            pmntstbs_currency, pmntstbs_multiplier, initial_date, type_pmntstbs, sht_pmntstbs)

#### File 13 - Easynvest ####
snvst_checking_clmn_conversion = {df_snvst_checking_raw_clmn_expect_lt[0]: date_clmn,
                                  df_snvst_checking_raw_clmn_expect_lt[1]: description_clmn,
                                  df_snvst_checking_raw_clmn_expect_lt[2]: value_original_clmn,
                                  df_snvst_checking_raw_clmn_expect_lt[3]: balance_clmn}
df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_snvst_checking, df_snvst_checking_raw_clmn_expect_lt,
                            snvst_checking_clmn_conversion, description_clmn, date_clmn, value_original_clmn,
                            currency_clmn, origin_clmn, snvst_checking_str, snvst_currency, snvst_multiplier,
                            initial_date, type_snvst_checking)

#### File 15 - Amazon ####
mzn_clmn_conversion = {df_mzn_raw_clmn_expect_lt[0]: date_clmn,
                       df_mzn_raw_clmn_expect_lt[1]: description_clmn,
                       df_mzn_raw_clmn_expect_lt[2]: value_original_clmn}
df_cashmov = fc.import_data(df_cashmov, df_cashmov_clmn_lt, flnm_mzn,
                            df_mzn_raw_clmn_expect_lt, mzn_clmn_conversion, description_clmn,
                            date_clmn, value_original_clmn, currency_clmn, origin_clmn, mzn_str,
                            mzn_currency, mzn_multiplier, initial_date, type_mzn, sht_mzn)

#### File 16 - Budgeting ####
df_manual_bgt = pd.read_excel(flnm_manual_bgt, sheet_name=sht_manual_bgt)
df_manual_bgt = df_manual_bgt.filter(items=df_manual_bgt_raw_clmn_expect_lt, axis=1)
manual_bgt_conversion = {df_manual_bgt_raw_clmn_expect_lt[0]: index_clmn,
                         df_manual_bgt_raw_clmn_expect_lt[1]: manual_bgt_clmn}
df_manual_bgt.rename(columns=manual_bgt_conversion, inplace=True)
df_manual_bgt.set_index(index_clmn, inplace=True)

df_auto_bgt = pd.read_excel(flnm_auto_bgt, sheet_name=sht_auto_bgt)
df_auto_bgt = df_auto_bgt.filter(items=df_auto_bgt_raw_clmn_expect_lt, axis=1)
auto_bgt_conversion = {df_auto_bgt_raw_clmn_expect_lt[0]: contain_clmn,
                       df_auto_bgt_raw_clmn_expect_lt[1]: auto_bgt_clmn,
                       df_auto_bgt_raw_clmn_expect_lt[2]: origin_clmn,
                       df_auto_bgt_raw_clmn_expect_lt[3]: initial_date_clmn,
                       df_auto_bgt_raw_clmn_expect_lt[4]: end_date_clmn}
df_auto_bgt[initial_date_clmn] = df_auto_bgt.apply(lambda x: pd.Timestamp(x[initial_date_clmn]), axis=1)
df_auto_bgt[end_date_clmn] = df_auto_bgt.apply(lambda x: pd.Timestamp(x[end_date_clmn]), axis=1)
df_auto_bgt.rename(columns=auto_bgt_conversion, inplace=True)

#############################################    STEP 3) BUDGETING    #############################################
[df_cashmov, df_cashmonv_not_assigned] = fc.assign_budgets(df_cashmov, df_manual_bgt, df_auto_bgt, bgt_l0_clmn,
                                                           date_clmn, manual_bgt_clmn, description_clmn, contain_clmn,
                                                           auto_bgt_clmn, origin_clmn, initial_date_clmn, end_date_clmn)

#############################################    STEP 5) OUTPUT    #############################################
# print(df_cashmov)
df_cashmov.to_csv(flnm_cashmov_csv)
df_cashmonv_not_assigned.to_csv(flnm_cashmov_not_assigned_csv)
# print(df_cashmov)