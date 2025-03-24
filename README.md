@ -0,0 +1,123 @@
import requests
import pymssql # type: ignore
import pyodbc # type: ignore
import json
import pandas as pd
import ast


# Replace with your API credentials
#client_id = 'collins_co'
#client_secret = 'ixcCfUQqtCsz9QOlIncV1x/5CITc/S1zMINmIlh1n/QI7bp+HYtaY+tEo9sDUq1Bm5H4+aHgco3pjk3Bw5HTvg=='

#Authenticate and Obtain Access Token
url = "https://auth.fastmarkets.com/connect/token"
#auth_url = 'https://api.fastmarkets.com/auth/token'


payload = {
 'grant_type': 'servicekey',
 'client_id': 'service_client',
 'scope': 'fastmarkets.physicalprices.api',
 'serviceName': 'service user',
 'serviceKey': 'service key'
 }
header = {'Content-Type': "application/x-www-form-urlencoded"}
token_response = requests.post(url, data = payload, headers = header)
accessToken = json.loads(token_response.content)
# Use token_response.text to get the response content as a string
token_response_content = token_response.text

# Parse the JSON response
parsed_response = json.loads(token_response_content)

# Extract the access token
access_token = parsed_response.get('access_token')

# Print the access token
print("Access Token:", access_token)

# Extract the access token and save it in a dictionary
access_token_dict = {'access_token': access_token}

#Bringing all available symbols related to a instrument visible to us:
symbols_url = "https://api.fastmarkets.com/Physical/v2/Instruments"
#query = {'symbols':'FP-LBR-0150', 'dates':'2025-03-13'} 
headers = {
 'Authorization': 'Bearer ' + access_token_dict['access_token'],
 'cache-control': 'no-cache'
 }

# Fetch all available symbols, symbol input can be used to retunr specific instruments:
req_symbols = requests.request("GET", symbols_url, headers=headers)
allSymbols = json.loads(req_symbols.content) #parsing JSON here and turning into Py dictionary

## Print the raw response to understand its structure. Printing.
#print("Raw API Response:", allSymbols)

#Saving Py dictionary in a DataFrame:
symbols_df=pd.DataFrame(allSymbols)

#Cleaning the df:
# Normalize the DataFrame to separate columns
symbols_df_normalized = pd.json_normalize(symbols_df.to_dict(orient='records'))


# Display the final DataFrame
print(symbols_df_normalized)


# Create a list of all symbols I have access to:
available_symbols = symbols_df_normalized['instruments.symbol'].tolist()

#below insert the list of symbols instead of one symbol
if available_symbols:
    prices_url = "https://api.fastmarkets.com/Physical/v2/Prices"
    query = {
        'symbols': ','.join(available_symbols),  # Convert list to comma-separated string
        'dates': ['2025-03-18','2025-3-13']
    }

    # Print the query to verify its contents
    print("Query:", query)
#request succeeseded!

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
