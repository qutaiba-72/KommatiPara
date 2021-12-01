# KommatiPara_APP

A very small company called KommatiPara that deals with bitcoin trading has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients. For this purpose, KommatiPara application has been created in order to enable the company to reach useful data for a new marketing push. The application conducts ETL process as follows:

* **Extract**: Reads two csv files from given paths and transform them into data frames.

* **Transform**: Filters one of the data frames according to selected countries, drops some columns which are unncessary for the company aim and renames columns for the easier readability to the business users. At the end, two data frames are joined according to the 'client_identifier' column. For the transformation process two generic functions which are 'filtering' and 'renaming' have been created.

* **Load**: Loads the output of the transformation to the given location.

In order to test data frame and column equalities in between the expected dataframe and transformed dataframes, **Chispa** method has been used. Application conduct out these tests successfully. Filtering and renaming functions work properly and generate expected results.

Application receives three arguments from the users which are:

1. The paths of the each datasets
2. Countries that users want to filter
3. Names that users want to give the columns

Additionally, the application keeps log records in order to search when unexpected problem occurs. Log records are kept and according to the rotating policy, the application rotates the log every day with a back up count of 5 

# Installation

## For pyspark

Spark is an open source project under Apache Software Foundation. Spark can be downloaded here: https://spark.apache.org/downloads.html

PySpark installation using PyPI is as follows: ```bash 
$ pip install pyspark ```


## For chispa test

Install the chispa with: 
```python 
pip install chispa 
```

# Configuration

##  Spark session

```python
import findspark

findspark.init() 
```

```python import pyspark
from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .master("local") \
    .appName("<give a name>") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate() 
```
    
## Chispa test 

```python 
from chispa.dataframe_comparer import *

import pytest

from chispa.column_comparer import assert_column_equality
```

##  Logging

```python 
import logging
import time

from logging.handlers import TimedRotatingFileHandler

def logger():
    logger = logging.getLogger('<Give the name for the log file>')

    logHandler = TimedRotatingFileHandler(filename="<Give the name for the log file>", when="D", interval=1, backupCount=5)
    logFormatter = logging.Formatter('%(asctime)s %(name)-12s %(message)s')
    logHandler.setFormatter(logFormatter)
    logger.setLevel(logging.INFO)


    logger.handlers:
    streamhandler = logging.StreamHandler()
    streamhandler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
    streamhandler.setFormatter(formatter)

    logger.addHandler(streamhandler)
    logger.addHandler(logHandler)

    return logger 

logger = logger()
```

## CSV file

```python
import csv
```

# Usage

After the installation is completed, you can clone my github repository in your computer. I used in this project Anaconda Navigator Jupyter Notebook. You can use my datasets or you can use your own datasets because in this app filtering and renaming functions are generic. The logic of this app is based on filtering datasets according to the selected countries. Therefore if you use your datasets, they should include country column. User should enter the paths of datasets which have to be same format as marked with yellow box in the picture and the application is created to read **CSV** file.  

![path](../main/images/path.png)

First aim of this app is to filter the first dataset according to the selected countries. For this purpose, filtering function has been created. First of all, User should enter how many countries user wants to filter. After that, User should choose the countries' number in the list. In our dataset there are 4 countries and we want to filter acoording to the United Kingdom and the Netherlands. Therefore user enter '2', you can see in the image marked with yellow box. Then, user enter '3' for the United Kingdom and '4' for the Netherlands as you can see in the image marked with red box. Function does not allow the user to enter a string value or a number greater than number of countries.

![path](../main/images/Filter.png)

After filtering, drop function has been used to remove personal identifiable information from the first dataset, excluding emails and cc_n column from the second dataset. Then renaming function has been created in order to rename the columns for the easier readability to the business users. To do this, renaming function request the new names for the columns. In our example, we change 'id' column as 'client_identifier', 'btc_a' as 'bitcoin_address' and 'cc_t' as 'credit_card_type' as marked in the image with yellow box. You can also see the change market with red box. If user do not want to change the name of column or columns, press only **enter** button. 

![path](../main/images/renaming.png)

Finally, When all the necessary change is done, app joins two datasets by using 'client_identifier' column as you can see in the image. Then the app request a location path the same format asa in the first image that user wants to save the dataset.

![path](../main/images/join.png)


# Challange

I have difficulty in importing functions from application file to test file. Jupyter notebook run the codes cell by cell. Therefore, whenever I call the function, it generates ERROR related to 'logging' because logging codes are in different cells. In order to solve this problem, I created logger function and send to the filtering function. Thus, whenever filtering function is called, logging function runs too. If someone has a different solution, please feel free to contribute to this project. 


# Contributing

It is open source project and anyone can contribute to this project. Any contributions you make are greatly appreciated.
