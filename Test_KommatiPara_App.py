#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


import pyspark
from pyspark.sql import SparkSession


spark = SparkSession     .builder     .master("local")     .appName("test_chispa")     .config("spark.some.config.option", "some-value")     .getOrCreate()


# In[3]:


#to import necessary libraries for chispa test

from chispa.dataframe_comparer import *

import pytest

from chispa.column_comparer import assert_column_equality


# In[4]:


from ipynb.fs.defs.Kommati_Para_App import filtering, logger, renaming


# In[5]:


#to create data frame in order to test filtering function.
def country_filter(*args):
    countries = [(1, 'Netherlands'),(2, 'Germany'),
             (3, 'France'),(4, 'Italy'),
             (5, 'Netherlands'),(6, 'Germany'),
             (7, 'France'),(8, 'Italy'),
             (9, 'Netherlands'),(10, 'United Kingdom'),
             (11, 'United Kingdom'),(12, 'Italy')]
    
    expected_countries = [(1, 'Netherlands'),(5, 'Netherlands'), 
                          (9, 'Netherlands'),(10, 'United Kingdom'),
                          (11, 'United Kingdom')]

    df_countries = spark.createDataFrame(countries, ["id", "country"])
    df_expected_countries = spark.createDataFrame(expected_countries, ["id", "country"])

    value = [row[0] for row in df_countries.select('country').distinct().collect()]
    key =[ x for x in range(1,(len(value)+1))]
    dict_countries = dict(zip(key,value) )
    
    return_data = dict(expected = df_expected_countries, base = df_countries, dict_filter = dict_countries)
    return return_data
    
    
return_data = country_filter()
df_expected_countries = return_data['expected']
df_countries = return_data['base']
dict_countries = return_data['dict_filter']

df_expected_countries.show()
df_countries.show()


# In[6]:


logger= logger()
df_filtered = filtering(df_countries, dict_countries, logger)
df_filtered.show()


# In[7]:


# to test data frame equality between expected data frame and filtered data frame.

assert_df_equality(df_expected_countries, df_filtered)


# In[8]:


#to create a new data frame by joining expected and filtered data frames in order to test column equality

#to rename 'country' column of filtered data frame in order to recognize the difference between before and after filtering

df_filtered = df_filtered.withColumnRenamed("country","filtered_country")
dataset_for_test = df_expected_countries.join(df_filtered,df_expected_countries.id == df_filtered.id,how = 'left')

#to test column equality of data frame
assert_column_equality(dataset_for_test, "country", "filtered_country")


# In[9]:


#to create a data frame in order to test renaming function
def column_rename(*args):
    personal_data = [(1, '1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2', 'visa-electron'),(2, '1Js9BA1rV31hJFmN25rh8HWfrrYLXAyw9T','jcb'),
    (3, '1CoG9ciLQVQCnia5oXfXPSag4DQ4iYeSpd','diners-club-enroute'),(4, '1DHTzZ7ypu3EzWtLBFiWoRo9svd1STMyrg','mastercard'),
    (5, '1LWktvit3XBCJNrsji7rWj2qEa5XAmyJiC','switch'),(6, '1HcqQ5Ys77sJm3ZJvxEAPLjCzB8LXVBTHU','visa'),
    (7, '1J1R1KuqtDDc3kzZmMZHUwmWSkD75F2kLB','maestro'),(8, '1M7ZbkpnUW8Eu9dKR4aCwaLQGUYshkfSdw','visa'),
    (9, '14HWxGsZ3nSctfvc7Y3DyGEt78jyuqRW97','visa'),(10, '1HkNSAP8Jm58DBiboFDsv916XJCYsfLrdg','jcb')]

    df_personaldata= spark.createDataFrame(personal_data, ["id", "btc_a","cc_t"])
    df_expected_personaldata = spark.createDataFrame(personal_data, ["client_identifier", "bitcoin_address","credit_card_type"])

    return_data = dict(expected = df_expected_personaldata, base = df_personaldata)
    return return_data

return_data = column_rename()
df_expected_personaldata = return_data['expected']
df_personaldata = return_data['base']

df_expected_personaldata.show()
df_personaldata.show()


# In[10]:


#Function for the renamimg
#to rename the columns for the easier readability to the business users

#id = client_identifier
#btc_a = bitcoin_address
#cc_t = credit_card_type

df_for_func = df_personaldata
datasets = renaming(df_for_func, df_personaldata, logger)
df_personaldata = datasets['dataset_2']


# In[11]:


# to test data frame equality between expected and renamed data frames

assert_df_equality(df_expected_personaldata, df_personaldata)


# In[ ]:




