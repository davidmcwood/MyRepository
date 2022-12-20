-- Databricks notebook source
-- MAGIC %md
-- MAGIC This notebook creates the Datalake database, tables and connections to the on-premise SQL server. The notebook is idempotent, so it's safe to run multiple times.
-- MAGIC 
-- MAGIC The on-premise tables involved are:
-- MAGIC 1. junifer_data_migration.ext_hh_eligible (read from on-premise SQL)
-- MAGIC 2. junifer_data_migration.ext_hh_meterdetails (read from on-premise SQL)
-- MAGIC 3. junifer_data_migration.ext_mighhmeterread (write to on-premise SQL)
-- MAGIC 4. junifer_data_migration.ext.migmpan (write to on-premise SQL)
-- MAGIC 5. junifer_data_migration.ext.migTargetCustomers (write to on-premise SQL)
-- MAGIC 6. junifer_data_migration.ext.DFKKKO (write to on-premise SQL)
-- MAGIC 7. junifer_data_migration.ext.DFKKOP (write to on-premise SQL)
-- MAGIC 8. junifer_data_migration.ext.DFKKZP (write to on-premise SQL)
-- MAGIC 9. junifer_data_migration.ext.FKKVKP (write to on-premise SQL)
-- MAGIC 10. junifer_data_migration.ext.MigBill (write to on-premise SQL)
-- MAGIC 11. junifer_data_migration.ext.MigAccountTransaction (write to on-premise SQL)
-- MAGIC 12. junifer_data_migration.dbo.AgedDebt (write to on-premise SQL)
-- MAGIC 13. junifer_data_migration.ext.MigAccount (write to on-premise SQL)
-- MAGIC 
-- MAGIC The migration SQL server is P-OVS16-PAN-01.eco.local (172.16.144.17) and the SQL service listens on the default port, 1433.

-- COMMAND ----------

-- DBTITLE 1,Create the database
CREATE DATABASE IF NOT EXISTS junifer_data_migration;

-- COMMAND ----------

-- DBTITLE 1,Create the JDBC connections to the on-premise tables
CREATE TABLE IF NOT EXISTS junifer_data_migration.ext_hh_eligible
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://172.16.144.17:1433",
  dbtable "DataMigrationLive.ext.hh_eligible",
  user "databricks",
  password "oVeYkwC80WOE"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS junifer_data_migration.ext_hh_meterdetails
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://172.16.144.17:1433",
  dbtable "DataMigrationLive.ext.hh_meterdetails",
  user "databricks",
  password "oVeYkwC80WOE"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS junifer_data_migration.ext_mighhmeterread
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://172.16.144.17:1433",
  dbtable "DataMigrationLive.ext.mighhmeterread",
  user "databricks",
  password "oVeYkwC80WOE"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS junifer_data_migration.ext_migmpan
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://172.16.144.17:1433",
  dbtable "DataMigrationLive.ext.migmpan",
  user "databricks",
  password "oVeYkwC80WOE"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS junifer_data_migration.ext_TargetCustomers
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://172.16.144.17:1433",
  dbtable "DataMigrationLive.ext.TargetCustomers",
  user "databricks",
  password "oVeYkwC80WOE"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS junifer_data_migration.ext_DFKKKO
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://172.16.144.17:1433",
  dbtable "DataMigrationLive.ext.DFKKKO",
  user "databricks",
  password "oVeYkwC80WOE"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS junifer_data_migration.ext_DFKKOP
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://172.16.144.17:1433",
  dbtable "DataMigrationLive.ext.DFKKOP",
  user "databricks",
  password "oVeYkwC80WOE"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS junifer_data_migration.ext_DFKKZP
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://172.16.144.17:1433",
  dbtable "DataMigrationLive.ext.DFKKZP",
  user "databricks",
  password "oVeYkwC80WOE"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS junifer_data_migration.ext_FKKVKP
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://172.16.144.17:1433",
  dbtable "DataMigrationLive.ext.FKKVKP",
  user "databricks",
  password "oVeYkwC80WOE"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS junifer_data_migration.ext_MigBill
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://172.16.144.17:1433",
  dbtable "DataMigrationLive.ext.MigBill",
  user "databricks",
  password "oVeYkwC80WOE"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS junifer_data_migration.ext_MigAccountTransaction
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://172.16.144.17:1433",
  dbtable "DataMigrationLive.ext.MigAccountTransaction",
  user "databricks",
  password "oVeYkwC80WOE"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS junifer_data_migration.dbo_AgedDebt
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://172.16.144.17:1433",
  dbtable "DataMigrationLive.dbo.AgedDebt",
  user "databricks",
  password "oVeYkwC80WOE"
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS junifer_data_migration.ext_MigAccount
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:sqlserver://172.16.144.17:1433",
  dbtable "DataMigrationLive.ext.MigAccount",
  user "databricks",
  password "oVeYkwC80WOE"
);

-- COMMAND ----------

SHOW TABLES IN junifer_data_migration

-- COMMAND ----------


