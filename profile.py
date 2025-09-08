import yfinance as yf
import pandas as pd


ticker = "AAPL"
stock = yf.Ticker(ticker)

info = stock.info


company_profile = {
    "Name": info.get("longName", ""),
    "Symbol": ticker,
    "Sector": info.get("sector", ""),
    "Industry": info.get("industry", ""),
    "Full Time Employees": info.get("fullTimeEmployees", ""),
    "Website": info.get("website", ""),
    "Address": info.get("address1", "") + ", " + info.get("city", "") + ", " + info.get("state", "") + " " + str(info.get("zip", "")) + ", " + info.get("country", ""),
    "Phone": info.get("phone", ""),
    "Description": info.get("longBusinessSummary", "")
}


profile_df = pd.DataFrame.from_dict(company_profile, orient="index", columns=["Value"])
print("Company Profile:")
print(profile_df)


executives = info.get("companyOfficers", [])

exec_list = []
for e in executives:
    exec_list.append({
        "Name": e.get("name", ""),
        "Title": e.get("title", ""),
        "Pay": e.get("totalPay", ""),
        "Exercised": e.get("exercisedValue", ""),
        "Year Born": e.get("yearBorn", "")
    })

executives_df = pd.DataFrame(exec_list)
print("\nKey Executives:")
print(executives_df)

with pd.ExcelWriter("AAPL_Profile.xlsx") as writer:
    profile_df.to_excel(writer, sheet_name="Company_Profile")
    executives_df.to_excel(writer, sheet_name="Key_Executives")

print("\nSaved company profile and executives to AAPL_Profile.xlsx")
