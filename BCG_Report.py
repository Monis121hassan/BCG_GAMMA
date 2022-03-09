#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 9

@author: monishassan
"""


import pyspark

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

""" Spark configuration"""

my_conf = SparkConf()
my_conf.set("spark.app.name", "SparkProject")


""" Spark Session"""

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()


""""Reading Datasets"""


df=spark.read.format("csv").option("header",True).option("inferschema",True).option("path", "/Users/monishassan/Downloads/Big Data Case study/Data/Primary_Person_use.csv").load()

vehichle_df=spark.read.format("csv").option("header",True).option("inferschema",True).option("path", "/Users/monishassan/Downloads/Big Data Case study/Data/Units_use.csv").load()

damaged_property_df=spark.read.format("csv").option("header",True).option("inferschema",True).option("path","/Users/monishassan/Downloads/Big Data Case study/Data/Damages_use.csv").load()


# Analysis 1  ---> To find number of crashes in which no of persons killed are male:



df2=df.select("PRSN_NBR","PRSN_GNDR_ID").filter(df.PRSN_GNDR_ID == 'MALE')

num_of_male_crash=df2.count()

print(num_of_male_crash)  


# Analysis 2 ---> Two wheelers booked for crashes

vehichle_df_2_wheeler=select("UNIT_NBR","VEH_BODY_STYL_ID").filter(df.VEH_BODY_STYL_ID == 'SPORT UTILITY VEHICLE').show()


# Analysis 3 ----> state have highest number of accidents in which female are involved



highest_state=df.filter(df.PRSN_GNDR_ID == 'FEMALE')

highest_state2=highest_state.groupBy("DRVR_LIC_STATE_ID").agg(count("DRVR_LIC_STATE_ID").alias("acc")).orderBy(col("acc").desc()).drop("acc").show(1)



# Analysis 4 --> top 15 VEH_MAKE_IDS that contribute to largest number of injuries



top_vehichle_df=vehichle_df.select("VEH_MAKE_ID","TOT_INJRY_CNT","DEATH_CNT")

top_vehichle_df_inc_death=top_vehichle_df.withColumn("Total_Injuries_death",col("TOT_INJRY_CNT") + col("DEATH_CNT"))

top_vehichle_df_gp=top_vehichle_df_inc_death.groupBy("VEH_MAKE_ID").agg(sum("Total_Injuries_death").alias("total injury deaths")).orderBy(col("total injury deaths").desc()).show(15)




# Analysis 5  ---> Body styled involved in crashes


vehichle_ethinic_df=vehichle_df.join(df,df.CRASH_ID==vehichle_df.CRASH_ID,"inner")      #--- Joining the primary_person and vehichle dataframes

vehichle_ethinic_df_filter=vehichle_ethinic_df.select("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID")

vehichle_ethinic_df_filter_gp=vehichle_ethinic_df.groupBy("PRSN_ETHNICITY_ID","VEH_BODY_STYL_ID").agg(count("PRSN_ETHNICITY_ID").alias("ethinicity")).orderBy(col("ethinicity").desc()).show(1)


# Analysis  6  ---> top 5 zipcodes with highest number of crashes alcohal being the contributing factor


crashed_car= vehichle_ethinic_df.select("VEH_BODY_STYL_ID","CONTRIB_FACTR_1_ID","DRVR_ZIP")

crahsed_car_alcohal= crashed_car.filter("VEH_BODY_STYL_ID == 'PASSENGER CAR, 4-DOOR' and CONTRIB_FACTR_1_ID=='UNDER INFLUENCE - ALCOHOL'").orderBy(col("DRVR_ZIP").desc()).show(5,truncate=False)



# Analysis 7  -----> count of disntict crash id's damage level is above 4

damaged_df=damaged_property_df.join(vehichle_df,damaged_property_df.CRASH_ID==vehichle_df.CRASH_ID,"inner").show(truncate=False)

damaged_df_Property=damaged_df.select("CRASH_ID","DAMAGED_PROPERTY","VEH_DMAG_SCL_1_ID").distinct()

damaged_df_Property_levels= damaged_df_Property.filter("DAMAGED_PROPERTY == 'None'")


damaged_df_Property_levels.filter("VEH_DMAG_SCL_1_ID == 'DAMAGED 5' and VEH_DMAG_SCL_1_ID == 'DAMAGED 6' ").show()












