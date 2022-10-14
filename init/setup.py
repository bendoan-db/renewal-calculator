# Databricks notebook source
dbutils.widgets.text("sfdc_id", "sfdc_id")
dbutils.widgets.text("rate_discount", "0.0")

# COMMAND ----------

discount = float(dbutils.widgets.get("rate_discount"))
sfdc_id = dbutils.widgets.get("sfdc_id")

# COMMAND ----------

daily_consumption = spark.sql(f"""
select accounts.Id    as id, 
         accounts.Name  as name,
         workloads.date as date,
         case
           when sku like "%_DLT_%"          then "dlt"
           when sku like "%_SQL_%"          then "sql"
           when sku like "%_JOBS_LIGHT_%"   then "jobs light"
           when sku like "%_JOBS_%"         then "jobs"
           when sku like "%_ALL_PURPOSE_%"  then "all-purpose"
           else sku
         end as sku,
         round( sum( workloads.revSharePrice * workloads.dbus ) ) as `dbu_revenue`,
         round( sum( workloads.dbus ) ) as dbus_total
    from sfdc.accounts as accounts
    join prod.workspaces as workspaces on workspaces.sfdcAccountId = accounts.Id
    join prod.workloads_sku_agg as workloads on workloads.workspaceId = workspaces.workspaceId
    where true
          and accounts.Id = {sfdc_id}
    group by 1, 2, 3, 4
    having true
          and date > now() - interval 365 day
    order by date asc,
             case 
               when sku = "all-purpose" then 1
               when sku = "jobs" then 2
               when sku = "sql" then 3
               when sku = "dlt" then 4
               when sku = "jobs light" then 5
               else 100
             end""")

# COMMAND ----------


