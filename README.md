# APIs
Sample API implementation
#Fetch all available Prices:
req_prices = requests.get( prices_url, headers=headers, data=query) #, params=params)
print("Response Status Code:", req_prices.status_code)
print("Response Content:", req_prices.json())


# Working but includes all low, mid and high prices, it also only includes symbols and not their tags which we need to:
# 1. Clean to just get Low prices
# 2. Left join to Instruments to get latest tags (if they change)
# #Parsing JSON here:
allPrices= json.loads(req_prices.content)
Prices_df=pd.DataFrame(allPrices)
prices_df_normalized=pd.json_normalize(Prices_df.to_dict(orient='records'))
print(prices_df_normalized)


#Extract low, mid, and high prices into a new dataframe

price_data=[]
for index, row in prices_df_normalized.iterrows():
    for entry in row["instruments.prices"]:
        price_data.append({
            "original_index": index,
            "symbol": row["instruments.symbol"],
            "date": entry["date"],
            "assessmentDate":entry["assessmentDate"],
            "revision": entry["revision"],
            "low": entry["low"],
            "mid":entry["mid"],
            "high":entry["high"]
        })

prices_df1=pd.DataFrame(price_data)

# Convert the date column to datetime
prices_df1['date'] = pd.to_datetime(prices_df1['date'])

# Group by symbol and get the row with the max date for each group
grouped_df = prices_df1.loc[prices_df1.groupby('symbol')['date'].idxmax()]
