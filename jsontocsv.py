import csv
import json
import pandas as pd


INPUT_JSON = "qc_output.json"
OUTPUT_CSV = "qc_output.csv"

def json_to_csv(input_file, output_file):
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    rows = []
    for item in data:
        company = item.get("Company", "")
        domain = item.get("Domain", "")
        technology = item.get("Technology", "")

        results = item.get("Result", [])
        if results:
            for r in results:
                rows.append({
                    "Company": company,
                    "Domain": domain,
                    "Technology": technology,
                    "Company_Name": r.get("company_name", ""),
                    "Website": r.get("website", ""),
                    "Country": r.get("country", ""),
                    "Product": r.get("product", ""),
                    "Detection_Confidence": r.get("detection_confidence", ""),
                    "Evidence": r.get("evidence", ""),
                    "Resource_Link": r.get("resource_link", ""),
                    "Date_Reference": r.get("date_reference", "")
                })
        else:

            rows.append({
                "Company": company,
                "Domain": domain,
                "Technology": technology,
                "Company_Name": "",
                "Website": "",
                "Country": "",
                "Product": "",
                "Detection_Confidence": "",
                "Evidence": "",
                "Resource_Link": "",
                "Date_Reference": ""
            })

    df = pd.DataFrame(rows)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"CSV saved to {output_file}")

if __name__ == "__main__":
    json_to_csv(INPUT_JSON, OUTPUT_CSV)
