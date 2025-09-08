import yfinance as yf
import pandas as pd

ticker = "AAPL"

stock = yf.Ticker(ticker)


income_annual = stock.financials
income_annual = income_annual.T  
income_annual.index = pd.to_datetime(income_annual.index).year 
print("Income Statement - Annual")
print(income_annual)


income_quarterly = stock.quarterly_financials
income_quarterly = income_quarterly.T
income_quarterly.index = pd.to_datetime(income_quarterly.index).date
print("\nIncome Statement - Quarterly")
print(income_quarterly)

balance_annual = stock.balance_sheet
balance_annual = balance_annual.T
balance_annual.index = pd.to_datetime(balance_annual.index).year
print("\nBalance Sheet - Annual")
print(balance_annual)


balance_quarterly = stock.quarterly_balance_sheet
balance_quarterly = balance_quarterly.T
balance_quarterly.index = pd.to_datetime(balance_quarterly.index).date
print("\nBalance Sheet - Quarterly")
print(balance_quarterly)

cashflow_annual = stock.cashflow
cashflow_annual = cashflow_annual.T
cashflow_annual.index = pd.to_datetime(cashflow_annual.index).year
print("\nCash Flow - Annual")
print(cashflow_annual)

cashflow_quarterly = stock.quarterly_cashflow
cashflow_quarterly = cashflow_quarterly.T
cashflow_quarterly.index = pd.to_datetime(cashflow_quarterly.index).date
print("\nCash Flow - Quarterly")
print(cashflow_quarterly)

with pd.ExcelWriter("AAPL_Financials.xlsx") as writer:
    income_annual.to_excel(writer, sheet_name="Income_Annual")
    income_quarterly.to_excel(writer, sheet_name="Income_Quarterly")
    balance_annual.to_excel(writer, sheet_name="Balance_Annual")
    balance_quarterly.to_excel(writer, sheet_name="Balance_Quarterly")
    cashflow_annual.to_excel(writer, sheet_name="CashFlow_Annual")
    cashflow_quarterly.to_excel(writer, sheet_name="CashFlow_Quarterly")

print("\nSaved all financial tables to AAPL_Financials.xlsx")
