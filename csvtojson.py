import pandas as pd
import json


input_path = r'C:\Users\propl\OneDrive\Desktop\work\OS_Keyword.csv'
output_path = r'C:\Users\propl\OneDrive\Desktop\work\os_keywords.json'


df = pd.read_csv(input_path)

keywords = ["OS"] + df['OS'].dropna().tolist()


with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(keywords, f, indent=4, ensure_ascii=False)

print(" JSON file saved at:", output_path)
