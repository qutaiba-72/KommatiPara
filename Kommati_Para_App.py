#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()


# In[2]:


#to import pyspark in order to create SparkSession
import pyspark
from pyspark.sql import SparkSession


spark = SparkSession     .builder     .master("local")     .appName("KommatiPara_App")     .config("spark.some.config.option", "some-value")     .getOrCreate()


# In[3]:


import logging
import time

from logging.handlers import TimedRotatingFileHandler

def logger():
    logger = logging.getLogger('kommatipara_log')

    logHandler = TimedRotatingFileHandler(filename="kommatipara_log", when="D", interval=1, backupCount=5)
    logFormatter = logging.Formatter('%(asctime)s %(name)-12s %(message)s')
    logHandler.setFormatter(logFormatter)
    logger.setLevel(logging.INFO)


    streamhandler = logging.StreamHandler()
    streamhandler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
    streamhandler.setFormatter(formatter)

    logger.addHandler(streamhandler)
    logger.addHandler(logHandler)

    return logger 

logger = logger()


# In[4]:


#Function for receiving paths of datasets
def paths():
    path1 = input("Please enter the path of first dataset that you want to work on: ")
    path2 = input("Please enter the path of second dataset that you want to work on: ")
    logger.info("INFO: The paths of the datasets have been received.")
    file_locations = dict(location1 = path1, location2 = path2)
    return file_locations

file_locations = paths()

file_location1 = file_locations['location1']
file_location2 = file_locations['location2']


# In[5]:


# to read first dataset
import csv
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
dataset_one = spark.read.format(file_type)   .option("inferSchema", infer_schema)   .option("header", first_row_is_header)   .option("sep", delimiter)   .load(file_location1)

display(dataset_one)
dataset_one.show()
logger.info("INFO: {} has been read.".format(file_location1))


# In[6]:


#to read second dataset

file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
dataset_two = spark.read.format(file_type)   .option("inferSchema", infer_schema)   .option("header", first_row_is_header)   .option("sep", delimiter)   .load(file_location2)

display(dataset_two)
dataset_two.show()
logger.info("INFO: {} has been read.".format(file_location2))


# In[7]:


#Function for filtering according to the selected countries
#In this assignment, clients data only from the United Kingdom or the Netherlands is used.

def filtering(var_1, var_2, logger):
    logger = logger
    dataset_one = var_1
    dic_of_country_name = var_2
    while True:
        try:
            while True:
                number_of_country =''
                total_country = len(dic_of_country_name)
                number_of_country = int(input("Please enter number of country between 1 and {} that you want to filter:  ".format(total_country)))
                if number_of_country > total_country or number_of_country < 1:
                    logger.warning('WARN: A number out of range has been entered [{}]'.format(number_of_country))
                    continue
                break
            break
        except (ValueError):
            logger.exception('An incorrect entry has been entered [{}]'.format(number_of_country))
            continue
            
    if number_of_country == total_country:
        logger.info('INFO: All countries has been selected')
    else:
        country_list = []
        for i in range(0,number_of_country):
            while True:
                try:
                    country_name = int(input('Please choose number of country [{}]: '.format(dic_of_country_name)))
                    if country_name not in dic_of_country_name:
                        logger.warning('WARN:  An incorrect entry has been entered [{}]'.format(country_name))
                        continue
                    else:
                        country_name = dic_of_country_name[country_name]
                        country_list.append(country_name)
                        logger.info('INFO: Country name [{}] has been added'.format(country_name))
                        break
                except (ValueError):
                    logger.exception('WARN: An incorrect entry has been entered')
                    continue
    if country_list == None:
        pass
    else:
        filter_formula = ''
        for i in range(len(country_list)):
            filter_formula = filter_formula + "(dataset_one.country == country_list[" + str(i)+ "]) | "
        filter_formula = eval(filter_formula[:-2]) 
    dataset_one = dataset_one.filter(filter_formula)
    return dataset_one

#to create a dictionary from 'country' column in order to choose country name for filtering
value = [row[0] for row in dataset_one.select('country').distinct().collect()]
key =[ x for x in range(1,(len(value)+1))]
dic_of_country_name = dict(zip(key,value) )

dataset_one = filtering(dataset_one, dic_of_country_name, logger)
dataset_one.show()
logger.info("INFO: Dataset_one has been filtered according to the selected countries.")


# In[8]:


#to remove personal identifiable information from the first dataset, excluding emails

dataset_one=dataset_one.drop("first_name","last_name","country")
logger.info("INFO: First name, last name and country columns have been dropped.")
dataset_one.show()


# In[9]:


# to remove credit card number column from the second dataset.

dataset_two=dataset_two.drop("cc_n")
logger.info("INFO: cc_n column has been dropped.")
dataset_two.show()


# In[10]:


#Function for the renamimg
#to rename the columns for the easier readability to the business users

#id = client_identifier
#btc_a = bitcoin_address
#cc_t = credit_card_type

def renaming(dataset_1,dataset_2, logger):
    dataset_one = dataset_1
    dataset_two = dataset_2
    
    #to create a list from the column names
    column_list = dataset_one.columns
    column_list2 = dataset_two.columns
    for i in range (len(column_list2)):
        if column_list2[i] not in column_list:
            column_list.append(column_list2[i])
        else:
            pass
        
    #to create a list in order to keep new names for the column    
    value_list = []
    for i in range (len(column_list)):
        rename_column = input('Please enter the new name for {} column: (If do not want to change, please press ENTER)'.format(column_list[i]))
        if rename_column == '':
            value_list.append(column_list[i])
        else:
            value_list.append(rename_column)
    logger.info("INFO: New names for columns have been received.")
    
    #to rename colums for dataset_one and dataset_two
    new_names = dict(zip(column_list,value_list))
    for key, value in new_names.items():
        dataset_one = dataset_one.withColumnRenamed(key,value)
        dataset_two = dataset_two.withColumnRenamed(key,value)
    logger.info("INFO: Columns names have been changed.")
    
    dataset_one.show()
    dataset_two.show()
    datasets = dict(dataset_1 = dataset_one, dataset_2 = dataset_two)
    return datasets

datasets = renaming(dataset_one,dataset_two, logger)
dataset_one = datasets['dataset_1']
dataset_two = datasets['dataset_2']


# In[11]:


# two datasets are joined according to the client_identifier 

EnhancedDataset = dataset_one.join(dataset_two,dataset_one.client_identifier == dataset_two.client_identifier).select(dataset_one.client_identifier, dataset_one.email, dataset_two.bitcoin_address, dataset_two.credit_card_type)
EnhancedDataset.show()
logger.info("INFO: Two datasets has been joined.")


# In[12]:


#the output of this assignment is saved in a client_data folder.
save_location = input('Please enter the save location ')

EnhancedDataset.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("sep", ",").save(save_location)
logger.info("INFO: Output has been saved in client_data folder.")


# In[ ]:




