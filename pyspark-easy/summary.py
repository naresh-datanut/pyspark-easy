from pyspark-easy.utils import *
from pyspark.sql import functions as F
from texttable import Texttable
from pyspark.sql import DataFrame

class pyspark_easy(object):
  
  def __init__(self, df):
    if not(isinstance(df, DataFrame)):
        print("oops! It is not a dataframe")
        exit
    self.df=df
    self.dfcount=df.count()
    self.dfdistcount=df.distinct().count()
    self.columns=df.columns
    self.no_of_columns=len(df.columns)
    self.col_types=df.dtypes
      
      
  def summary(self):
    
  # count no.of rows, variables and columns
  
    t = Texttable()
  
    t.add_rows([['The number of observations/rows :',  self.dfcount], 
                ['the number of variables/columns :', self.no_of_columns],
                ["No of Duplicate rows :", str(self.dfcount-self.dfdistcount)],
                ["% of Duplicate rows :", str(round((1-((self.dfdistcount/self.dfcount)))*100,10))]],header=False)
  
    print(t.draw())
 
 # list out the columns by types
  
    string=0
    numeric=0
    date_time=0
    other_type=0
    string_columns=[]
    numeric_columns=[]
    date_time_columns=[]
    other_type_columns=[]
    for i in self.col_types:
      if len(i)>1:
        if 'string' in i[1] or 'char' in i[1]:
          string=string+1
          string_columns.append(list(i))
        elif 'time' in i[1] or 'date' in i[1]:
          date_time=date_time+1
          date_time_columns.append(list(i))
        elif 'decimal' in i[1] or 'double' in i[1] or 'int' in i[1] or 'float' in i[1]:
          numeric=numeric+1
          numeric_columns.append(list(i))
        else:
          other_type=other_type+1
          other_type_columns.append(list(i))
    print("\n")
    print("The number of string columns are :", string)
    if string>0:
      print("\n")
      print("The string columns are :")
      t = Texttable()
      string_columns.insert(0,['Columns','Types'])
      t.add_rows(string_columns)
      print(t.draw())
    print("\n")
    print("The number of numerical columns are :", numeric)
    if numeric>0:
      print("\n")
      print("The numerical columns are :")
      t = Texttable()
      numeric_columns.insert(0,['Columns','Types'])
      t.add_rows(numeric_columns)
      print(t.draw())
    print("\n")
    print("The number of date/time columns are :", date_time)
    if date_time>0:
      print("\n")
      print("The date/time columns are :")
      t = Texttable()
      date_time_columns.insert(0,['Columns','Types'])
      t.add_rows(date_time_columns)
      print(t.draw())
    if other_type>0:
     print("\n")
     print("The number of columns with other data types are :", other_type)
     print("\n") 
     print("The other columns with data types are :")
     t = Texttable()
     other_type_columns.insert(0,['Columns','Types'])
     t.add_rows(other_type_columns)
     print(t.draw())

  # summary stats for numerical columns
  
    numeric_cols=Extract(numeric_columns)[1:]
    if len(numeric_cols)>0:
      print("\n")
      print("Summary statistics for numerical columns are :")
      print("\n")
      for i in range(0,len(numeric_cols),5):
        dff=self.df.select([F.col(c) for c in numeric_cols[i:i+5]])
        dff.summary().show()
 
 # missing values
  
    miss_values=self.df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in self.columns]).toPandas()
    miss_values=miss_values.loc[:, (miss_values != 0).any(axis=0)]
    miss_c=list(miss_values)
    if len(miss_c)<=0:
     print("\n")
     print("There are no columns with missing values")
    else:
      miss_v=list(miss_values.values)[0]
      percent_values=[]
      miss_main_list=[]
      for i in range(len(miss_c)):
        miss_sub_list=[]
        miss_sub_list.append(miss_c[i])
        miss_sub_list.append(miss_v[i])
        miss_sub_list.append(round((miss_v[i]/self.dfcount)*100,2))
        miss_main_list.append(miss_sub_list)
      print("\n")
      print("Number of columns with missing values are :", len(miss_c))
      print("\n")
      print("The columns with missing values are :")
      print("\n")
      t = Texttable()
      miss_main_list.insert(0,['Columns','No. of missing values','% of missing values'])
      t.add_rows(miss_main_list)
      print(t.draw())

    