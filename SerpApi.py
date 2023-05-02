# Install the below packages before importing
from serpapi import GoogleSearch
import pandas as pd
from flatten_json import flatten

# Loading the keyword data
kw = pd.read_csv('keyword.csv')
#print(kw)
keyword = kw.KEYWORD_NAME.values.tolist()
print(keyword)

# For testing we can use this as it will take 2-3 hrs to pull this data
df = kw.head(5)
keyword = df.KEYWORD_NAME.values.tolist()

# Initiating the SERP API request search providing the parameters
search = GoogleSearch({
    "location": "Austin,Texas",
    # "async": True,
    "api_key": "ccbf0b353405970af73c22b101bf38f045d580de6c217991c7c324c0f67581a5",
    "num": 100
})

# Creating the blank dataframe to append the data for each keyword
serp_data = pd.DataFrame()

# Looping through the keyword and storing the data into the above dataframe
for kw_to_search in keyword:
    search.params_dict["q"] = kw_to_search
    result = search.get_dict()
    organic_result = result['organic_results']
    print("New KW")
    dic_flattened = [flatten(d) for d in organic_result]
    df = pd.DataFrame(dic_flattened)
    serp_data = serp_data.append(df)

# Writing the above dataframe into the csv file.
serp_data.to_csv('serpdata.csv' ,index = False, sep=',')
print('data exported successfully')