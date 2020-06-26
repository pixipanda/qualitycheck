Quality Check 
=====================================
```text
This Project is used for data validation checks.
Every job must do data quality checks of their data.
Every job must define a config file for its data quality check
This project implemented using Spark
```



Config file template
---------------------------------
```text
src/test/resources/application.conf
Create a config file for each job in your project and put appropriate checks.
sources config parameter is an array. So you can have data quality checks for multiple tables.
```
```hocon
qualityCheck {
  sources = [
    {
      type = "hive"
      dbName = "db1"
      tableName = "table1"
      query = "query1"
      checks {
        rowCountCheck {
          count = 0,
          relation = "gt"
        }
        nullCheck = [ "colA", "colB",  "colC", "colD"]
        uniqueChecks = [
          ["colA", "colB",  "colC", "colD"],
          ["colX", "colY",  "colZ"]
          ["colM", "colN"]
        ]
        distinctChecks = [
          {columns = ["colA"], count = 1600, relation = "ge"},
          {columns = ["colZ", "colB"], count = 1, relation = "ge"},
          {columns = ["colY"], count = 1, relation = "ge"},
          {columns = ["colM"], count = 1, relation = "ge"}
        ]
      }
    },
    {
      type = "teradata"
      dbName = "db2"
      tableName = "table2"
      checks {
        rowCountCheck {
          count = 0,
          relation = "gt"
        }
        nullCheck = ["colA", "colB",  "colC", "colD"]
        uniqueChecks = [
          ["colA", "colB",  "colC", "colD"],
          ["colX", "colY",  "colZ"]
          ["colM", "colN"]
        ]
        distinctChecks = [
          {columns = ["colA"], count = 1600, relation = "ge"},
          {columns = ["colZ", "colB"], count = 1, relation = "ge"},
          {columns = ["colY"], count = 1, relation = "ge"},
          {columns = ["colM"], count = 1, relation = "ge"}
        ]
      }
    }
  ]
}
```



Important Note
-------------------------------
```text
If query field is specified all checks will be done on the query data else all the checks will be done on the source data
```



Config details
-------------------------------
```text
sourceQualityCheck: All the checks for a source must be specified inside this keyword
type: type is used to identify a source. It could be Hive, Teradata, csv, orc etc. Currently Hive is supported
dbName: Database name
tableName: Table name
It will check if table exists. If not then throws an error and exits the program
query = Any query that you want to run on this table. Checks will be done on the data of this query


checks:
  rowCountCheck = { count = 0, relation = "gt" }
  It will compute the total row count for the given data. If the count does not match the relation it will throw an error and exits the program.
  Supported relational operators gt, ge, lt, le, and eq


  nullCheck = [ "colA", "colB",  "colC", "colD"]
  Checks whether these columns contain null. If it contains null then throws an error and exits the program.
  
  
  uniqueChecks = [
    ["colA", "colB",  "colC", "colD"],
    ["colX", "colY",  "colZ"]
    ["colM", "colN"]
  ]
  Check whether duplicate records exist for the given combination of columns. 
  If duplicate exists then throws a error and exits the program
  
  
  distinctChecks = [
    {columns = ["colA"], count = 1600, relation = "ge"},
    {columns = ["colZ", "colB"], count = 1, relation = "ge"},
    {columns = ["colY"], count = 1, relation = "ge"},
    {columns = ["colM"], count = 1, relation = "ge"}
  ]
  Checks the distinct count of all the specified columns. 
  If it does not match then throws an error and exits the program.
  relation here specifies whether the expected count must be greater than(gt) the specified count, etc
  Supported relational operators gt, ge, lt, le, eq
```



Class Name
-------------------------------
```scala
com.pixipanda.qualitycheck.jobs.QualityCheck
```





How to run this program
-------------------------------
```text
Local: Run this test case to get a demo of the framework.
src/test/scala/com/pixipanda/qualitycheck/utils/ComputeChecksSpec.scala
Custer:
Please do create a shell script to submit spark job in your project and possibly include that as an action in your workflow.xml
```
```xml
<action name="qualityCheck">
    <ssh xmlns="uri:oozie:ssh-action:0.2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="uri:oozie:ssh-action:0.2 ">
        <host>${applicationUserName}@${hostName}</host>
        <command>${qualityCheckSparkSubmitScript}</command>
        <arg>${appName}</arg>
        <arg>${qualityCheckClassName}</arg>
        <arg>${master}</arg>
        <arg>${deployMode}</arg>
        <arg>${queueName}</arg>
        <arg>${qualityCheckConfigFile}</arg>
        <arg>${qualityCheckJarLocation}</arg>
    </ssh>
    <ok to="end"/>
    <error to="kill"/>
</action>
```

