# pyspark-easy
pyspark-easy is a library to get quick insights on data in Spark DataFrame. It is also included with functions aimed to assist users in their model development by eliminating their need of doing repetitive tasks.
 
## Installing:
Install from pypi:
```
pip install pyspark-easy
```
Or directly from github:
```
git clone https://github.com/naresh-datanut/pyspark-easy.git
cd pyspark-easy
pip install .
```

## Functions:

 - pyspark_easy(dataframe) - This class loads the dataframe and computes the stats. The function pyspark_easy(dataframe).summary(*args) retreives the essential insights on dataframe in a more simple way. 

#### example:
```
from pyspark_easy import pyspark_easy
py=pyspark_easy(dataframe)
py.summary(*summary_stats)
```
The summary stats are turned off by default. py.summary('Y') renders the summary status for the numerical columns.
```
+-----------------------------------+----------+
| The number of observations/rows : | 22312689 |
+-----------------------------------+----------+
| the number of variables/columns : | 10       |
+-----------------------------------+----------+
| No of Duplicate rows :            | 0        |
+-----------------------------------+----------+
| % of Duplicate rows :             | 0        |
+-----------------------------------+----------+

The number of string columns are : 6


The string columns are :
+-------------------+--------+
|      Columns      | Types  |
+===================+========+
| business_dt       | string |
+-------------------+--------+
| branchid          | string |
+-------------------+--------+
| customerno        | string |
+-------------------+--------+
| phone             | string |
+-------------------+--------+
| customername      | string |
+-------------------+--------+
| customerid        | string |
+-------------------+--------+



The number of numerical columns are : 3


The numerical columns are :
+--------------------+--------+
|      Columns       | Types  |
+====================+========+
| id                 | bigint |
+--------------------+--------+
| amount             | bigint |
+--------------------+--------+
| average_bal        | int    |
+--------------------+--------+



The number of date/time columns are : 1


The date/time columns are :
+-----------+-----------+
|  Columns  |   Types   |
+===========+===========+
| updatedon | timestamp |
+-----------+-----------+


Number of columns with missing values are : 3


The columns with missing values are :


+-------------------+-----------------------+---------------------+
|      Columns      | No. of missing values | % of missing values |
+===================+=======================+=====================+
| customername      | 64029                 | 0.290               |
+-------------------+-----------------------+---------------------+
| customerid        | 11                    | 0                   |
+-------------------+-----------------------+---------------------+
| id                | 11                    | 0                   |
+-------------------+-----------------------+---------------------+
```
 - column_search(spark_session,column_name,*schema) - This function will search column name across all the schemas or the schemas mentioned, in the lake. column_name is built as Like operator, so it retrieves all the columns that contains the column_name mentioned, along with table name and schema. Schema is also built as Like operator. It also catches and list out the kudu table where the search cannot be performed. It does separate and list out the schemas where the user doesn't have permission.

#### example:
```
from pyspark_easy import column_search
column_search(spark_session,'average','sb') - searches for columns that contains the word 'average' on the schemas that starts with the name 'sb'.
or 
column_search(spark_session,'average',['sb','dp']) - searches for columns that contains the word 'average' on the schemas that starts with the name 'sb' or 'dp'.
or 
column_Search(spark_session,'average') - searches for columns that contains the word 'average' on all the schemas in the lake.
```

- dates_generator(date,column_name,forward, backward) - This function help in feature engineering on building time series data. One common requirement in feature engg is to build one feature for each month calculating the average of last 12 months. This does that quickly. 

#### example:
```
from pyspark_easy import dates_generator
dates_generator('2020-03-01','customer_average_balance',12,0)

[['2021-02-01', '2021-02-28', 'customer_average_balance_b1'],
 ['2021-01-01', '2021-01-31', 'customer_average_balance_b2'],
 ['2020-12-01', '2020-12-31', 'customer_average_balance_b3'],
 ['2020-11-01', '2020-11-30', 'customer_average_balance_b4'],
 ['2020-10-01', '2020-10-31', 'customer_average_balance_b5'],
 ['2020-09-01', '2020-09-30', 'customer_average_balance_b6'],
 ['2020-08-01', '2020-08-31', 'customer_average_balance_b7'],
 ['2020-07-01', '2020-07-31', 'customer_average_balance_b8'],
 ['2020-06-01', '2020-06-30', 'customer_average_balance_b9'],
 ['2020-05-01', '2020-05-31', 'customer_average_balance_b10'],
 ['2020-04-01', '2020-04-30', 'customer_average_balance_b11'],
 ['2020-03-01', '2020-03-31', 'customer_average_balance_b12']]
 
 or 
 
 dates_generator('2021-03-01','customer_average_balance',6,6)
[['2021-02-01', '2021-02-28', 'customer_average_balance_b1'],
 ['2021-01-01', '2021-01-31', 'customer_average_balance_b2'],
 ['2020-12-01', '2020-12-31', 'customer_average_balance_b3'],
 ['2020-11-01', '2020-11-30', 'customer_average_balance_b4'],
 ['2020-10-01', '2020-10-31', 'customer_average_balance_b5'],
 ['2020-09-01', '2020-09-30', 'customer_average_balance_b6'],
 ['2021-04-01', '2021-04-30', 'customer_average_balance_f1'],
 ['2021-05-01', '2021-05-31', 'customer_average_balance_f2'],
 ['2021-06-01', '2021-06-30', 'customer_average_balance_f3'],
 ['2021-07-01', '2021-07-31', 'customer_average_balance_f4'],
 ['2021-08-01', '2021-08-31', 'customer_average_balance_f5'],
 ['2021-09-01', '2021-09-30', 'customer_average_balance_f6']]
 ```
 #### example of calling it inside pyspark code:
 ```
 dates=dates_generator('2021-03-01','customer_average_balance',6,0)
 for col in dates:
         df = df.withColumn(
                    col[2],
                    F.when(
                        (
                            (df.business_dt >= F.lit(col[0]))
                            & (df.business_dt <= F.lit(col[1])
                        )),
                        avg(df.customer_averge_balance)
                    ).otherwise(0),
                )
```
- pyspark_model_eval(model,predicted_df) - This class loads the model and the dataframe containing prediction results. Ir evaluates the model and provides the quick results for binary and multi-class classification models.

#### example:
```
from pyspark_easy import pyspark_model_eval
res=pyspark_model_eval(model,predicted_df)
res.results()
```

The function .results() yields the results in a few seconds.

#### example of binary classification model:
```
The Train vs Test evaluation Metrics are :
+--------------------+--------+--------+
|      Metrics       | Train  |  Test  |
+====================+========+========+
| AreaUnderROC       | 88.430 | 79.980 |
+--------------------+--------+--------+
| Accuracy           | 80.070 | 80.110 |
+--------------------+--------+--------+
| Weighted Precision | 80.110 | 80.630 |
+--------------------+--------+--------+
| Weighted Recall    | 80.070 | 80.110 |
+--------------------+--------+--------+
| Weighted F1 Score  | 80.010 | 79.990 |
+--------------------+--------+--------+
Note: The Weighted metrics - calculated by label then it is averaged

All the below graphs plotted in a way it is easier to understand :-
Confusion matrix
Training ROC curve
Training PR curve
Test ROC curve
Test PR Curve
Test ROC Curve by label
Test PR Curve by label
Lift chart
Cumulative chart

The binary classification metrics for test data are:
+-----------+-----------+
|  Metrics  | Test Data |
+===========+===========+
| Precision | 84.560    |
+-----------+-----------+
| Recall    | 72.790    |
+-----------+-----------+
| F1 Score  | 78.240    |
+-----------+-----------+


The Classification report in a tabular format.
```

