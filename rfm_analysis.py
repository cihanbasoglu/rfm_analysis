import pandas as pd
import pydata_google_auth
from google.cloud import bigquery

credentials = pydata_google_auth.get_user_credentials(["https://www.googleapis.com/auth/cloud-platform"])
client = bigquery.Client(credentials=credentials, project='YOUR_PROJECT')

def run_query(query: str, client):
    query_job = client.query(query)
    return pd.DataFrame([dict(row) for row in query_job])

query = ("""
SELECT
userid,
count(userid) as frequency,
sum(price) as monetary,
min(DATE_DIFF(DATE current_date, DATE_TRUNC(date(MemberCreditChargeDate), DAY), DAY)) as recency
FROM YOUR_TABLE
where price > 0
and MemberCreditChargeDate IS NOT NULL
group by userid
""")
df = run_query(query, client)

df.to_csv('query_results_df.csv')
data = pd.read_csv('query_results_df.csv')

# describe the initial data
frequency_mean = data['frequency'].mean()
frequency_min = data['frequency'].min()
frequency_max = data['frequency'].max()
monetary_mean = data['monetary'].mean()
monetary_min = data['monetary'].min()
monetary_max = data['monetary'].max()
monetary_total = data['monetary'].sum()
recency_mean = data['recency'].mean()
recency_min = data['recency'].min()
recency_max = data['recency'].max()
user_count = data['userid'].nunique()
print("Frequency Mean:", frequency_mean)
print("Frequency Min:", frequency_min)
print("Frequency Max:", frequency_max)
print("Monetary Mean:", monetary_mean)
print("Monetary Min:", monetary_min)
print("Monetary Max:", monetary_max)
print("Monetary Total:", monetary_total)
print("Recency Mean:", recency_mean)
print("Recency Min:", recency_min)
print("Recency Max:", recency_max)
print("User Count:", user_count)


rfm_quartiles = data.quantile(q=[0.25, 0.5, 0.75])
rfm_quartiles = rfm_quartiles.to_dict()

def FMScore(x, c, quartiles):
    if x <= quartiles[c][0.25]:
        return 1
    elif x <= quartiles[c][0.50]:
        return 2
    elif x <= quartiles[c][0.75]:
        return 3
    else:
        return 4

# scoring is inverted for the recency so that lower values are assigned higher scores
def RScore(x, c, quartiles):
    if x <= quartiles[c][0.25]:
        return 4
    elif x <= quartiles[c][0.50]:
        return 3
    elif x <= quartiles[c][0.75]:
        return 2
    else:
        return 1

# Creating scores
data['F'] = data['frequency'].apply(FMScore, args=('frequency',rfm_quartiles,))
data['R'] = data['recency'].apply(RScore, args=('recency',rfm_quartiles,))
data['M'] = data['monetary'].apply(FMScore, args=('monetary',rfm_quartiles,))

# Define RFM Segments and Scores
data['RFM_Segment'] = data['R'].map(str) + data['F'].map(str) + data['M'].map(str)
data['RFM_Score'] = data[['R', 'F', 'M']].sum(axis=1)

# Segments based on RFM score
def assign_gaming_segment(row):
    R, F, M = row['R'], row['F'], row['M']
    if R >= 3 and F >= 3 and M >= 3:
        return 'Champions'
    elif R >= 2 and F >= 3 and M >= 2:
        return 'Loyalists'
    elif M >= 3 and (F < 3 or R < 3):
        return 'Big Spenders'
    elif R <= 2 and F >= 3 and M >= 2:
        return 'At Risk'
    elif R >= 3 and (F < 2 or M < 2):
        return 'Newbies'
    elif (R == 2 or F == 2 or M == 2) and not (R < 2 and F < 2 and M < 2):
        return 'Need Attention'
    elif (F == 3 or M == 3) and R < 3:
        return 'Promising'
    elif R <= 1 and F <= 1 and M <= 1:
        return 'Churning'
    else:
        return 'Engage More' 

data['Gaming_Segment'] = data.apply(assign_gaming_segment, axis=1)

def to_bq(df, project, dataset, table, credentials, if_exists):
    df.fillna('NA', inplace=True)
    df.to_gbq(f'{dataset}.{table}', f'{project}', credentials=credentials, if_exists=if_exists)
    return print('File is uploaded')

project = 'YOUR_PROJECT'
dataset = 'YOUR_DATASET'
table = 'YOUR_TABLE'

to_bq(data,project,dataset,table,credentials,'append')
