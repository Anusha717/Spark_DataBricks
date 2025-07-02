# Databricks notebook source
spark


# COMMAND ----------

# MAGIC %md
# MAGIC **CHURN RISK = User Inactive for 30 days(not logged in )**

# COMMAND ----------

user_data= [
("U1","2025-04-15"),
("U2","2025-03-01"),
("U3","2024-11-01"),
("U4","2025-02-27"),
("U1","2025-04-24"),
("U5","2025-05-01"),
("U1","2025-03-11"),
("U2","2025-12-05"),
("U2","2024-05-09"),
("U4","2025-01-30"),
("U1","2025-04-20"),
("U5","2025-02-05"),
("U3","2025-02-15")
]
from pyspark.sql.functions import col,to_date,max,date_diff,lit,when
user_login_DF = spark.createDataFrame(user_data).toDF("user_id","login_date").withColumn("login_date",to_date(col("login_date"),"yyyy-MM-dd"))
user_login_DF.show()

# COMMAND ----------

User_last_login=user_login_DF.groupBy("user_id").agg(max(col("login_date")).alias("last_login_date"))
User_last_login.show()
Churn_Risk=User_last_login.withColumn("Inactive_days",date_diff(lit("2025-05-11"),col("last_login_date"))).withColumn("ChurnRisk",when(col("Inactive_days")>=30,"High").otherwise("Low"))
Churn_Risk.show()


# COMMAND ----------

#Spark SQL Approach
user_login_DF.createOrReplaceTempView("user_data_table")
user =spark.sql("""select user_id,max(login_date) as last_login, date_diff('2025-05-11',last_login) as Inactive_days,
                case when Inactive_days >=30 then 'High' else 'Low' end as ChurnRisk
from user_data_table group by user_id""");
user.show()