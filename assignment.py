#!/usr/bin/env python
# coding: utf-8

# ### Requirement ###
# 
# Given the Github API (https://docs.github.com/en/free-pro-team@latest/rest), create a simple ETL pipeline. 
# For a particular repository, example Airflow (https://github.com/apache/airflow), pull in the commits over the last 6 months. (plus points if this window of time can be varied) 
# With the data ingested, address the follow queries: 
# 
# 1. For the ingested commits, determine the top 5 committers ranked by count of commits and their number of commits. 
# 2. For the ingested commits, determine the committer with the longest commit streak.  
# 3. For the ingested commits, generate a heatmap of number of commits count by all users by day of the week and by 3 hour blocks. 
# 
# Sample heatmap
#      00-03 	03-06 	06-09 	09-12 	12-15 	15-18 	18-21 	21-00
# Mon 	 	 	 	 	 	 	 	 
# Tues 	 	 	 	 	 	 	 	 
# Wed 	 	 	 	 	 	 	 	 
# Thurs 	 	 	 	 	 	 	 	 
# Fri 	 	 	 	 	 	 	 	 
# Sat 	 	 	 	 	 	 	 	 
# Sun 	 	 	 	 	 	 	 	 
#  
# 


import json
import requests
import numpy as np
import pandas as pd


from datetime import datetime
from dateutil.relativedelta import relativedelta

# ask user for number of months data to retrieve
num_months = input("Please Enter Number of Months you want to retrieve data:")
# calculate the time base on number of months
since_months = datetime.now() + relativedelta(months=-int(num_months))
since = since_months.isoformat()
print(since)

# get all commit data page by page with default 100 per page,filter committer information
page = 1
committer_info = []
while (True):
    param = {"per_page": 100, "since": since, "page": page}
    header = {"Authorization": "Bearer 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"}
    data = requests.get('https://api.github.com/repos/apache/airflow/commits', params=param, headers=header)
    json_result = data.json()
    result_count = len(json_result)
    # print(result_count)
    if (result_count < 100):
        break
    else:
        page = page + 1
    for committer in json_result:
        # here i retrieve commit author information
        committer_info.append(committer["commit"]["author"])

# convert data into data frame
df = pd.DataFrame(data=committer_info)
# save data into csv file for further analysis (ideally should insert data into DB, but here we use file)
df.to_csv("result.csv", index=False)

# load the data into data frame
df = pd.read_csv('result.csv')

# find top 5 committers ranked by count of commits and their number of commits
top_committer = df['name'].value_counts().head()
print('Top 5 committer base on commit count:\n', top_committer)


# For the ingested commits, determine the committer with the longest commit streak
# convert date column to date time type
df['date'] = pd.to_datetime(df['date'])
# find longest streak
long_streak = df.groupby('name')['date'].apply(lambda x: x.max() - x.min()).sort_values(ascending=False).head(1)
# get the committer name and time
print('User {0} has longest commit streak {1}'.format(long_streak.index[0], long_streak.values[0]))


# generate a heatmap of number of commits count by all users by day of the week and by 3 hour blocks
# create day_of_week column and hour column
df['day_of_week'] = df['date'].dt.day_name()
df['hour'] = df['date'].dt.hour
df.head(15)


# 3 hour block column
# define a function to convert hour into 3 hour block
# 00-03 03-06 06-09 09-12 12-15 15-18 18-21 21-00
def convert_hour_block(hour):
    if hour < 3:
        return '00-03'
    elif hour < 6:
        return '03-06'
    elif hour < 9:
        return '06-09'
    elif hour < 12:
        return '09-12'
    elif hour < 15:
        return '12-15'
    elif hour < 18:
        return '15-18'
    elif hour < 21:
        return '18-21'
    elif hour < 24:
        return '21-00'


df['hour_block'] = df['hour'].apply(convert_hour_block)
df.head(15)

# generate heatmap
heatmap = pd.crosstab(df['day_of_week'], df['hour_block'])
print(heatmap)
