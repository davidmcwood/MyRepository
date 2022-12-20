-- Databricks notebook source
-- MAGIC %md
-- MAGIC This notebook should be executed following ext.Mod14_PopulateHHRefTbls_usp and complete before ext.Mod15_FinalBillFlag_usp starts.
-- MAGIC 
-- MAGIC Creation of HHMeterRead records with 0 (zero) consumption is neccessary, where D0275 period does not exist for any given period for MPAN and Measure.  Date ranges for MPAN/measures can be found in HH_MeterDetails object
-- MAGIC 
-- MAGIC Final step is to export files to S3 bucket, where files are picked up automtically by on-prem extraction process.

-- COMMAND ----------

USE junifer_data_migration;

-- COMMAND ----------

--This table is just used within this notebook and then transferred to an on-premise location. It can be deleted if needed.
CREATE
OR REPLACE TABLE mig_hh_meter_read (
  HH_MR_Identifier string NOT NULL,
  MPxN_FK string,
  MeterRegisterIdentifier string NOT NULL,
  ReadingDate string NOT NULL,
  Consumption decimal(9, 1) NOT NULL,
  MetricUnit string,
  SequenceType string,
  ReadingQuality string NOT NULL,
  ReadingSource string NOT NULL,
  ReadingStatus string NOT NULL,
  RuleFailureID int,
  RelationshipID int,
  GlideFl int NOT NULL,
  NotMigrateFl int NOT NULL
) USING DELTA LOCATION 's3a://eco-datalake-production/sandbox/junifer-data-migration/mig-hh-meter-read'

-- COMMAND ----------

CREATE 
OR REPLACE TABLE ext_MigMpan_lcl
(
MPAN_Identifier string NOT NULL,
MPAN_CORE string NOT NULL,
BillableFl string NOT NULL,
Property_Identifier string NOT NULL,
Supply_Status string NOT NULL,
RuleFailureID int,
RelationshipID int,
GlideFl int NOT NULL,
NotMigrateFl int NOT NULL
) USING DELTA LOCATION 's3a://eco-datalake-production/sandbox/junifer-data-migration/mig-mpan-lcl'


-- COMMAND ----------

-- Insert in to local version of MigMpan
INSERT INTO
  ext_MigMpan_lcl
SELECT
    MPAN_Identifier,
    MPAN_CORE,
    BillableFl,
    Property_Identifier,
    Supply_Status,
    RuleFailureID,
    RelationshipID,
    GlideFl,
    NotMigrateFl
FROM ext_migmpan;

-- COMMAND ----------

-- CREATE 
-- OR REPLACE TABLE ext_TargetCustomers_lcl
-- (
-- RelationshipID int
-- ) USING DELTA LOCATION 's3a://eco-datalake-production/sandbox/junifer-data-migration/TargetCustomers-lcl'

-- COMMAND ----------

-- Insert in to local version of MigMpan
-- INSERT INTO
--   ext_TargetCustomers_lcl
-- SELECT
--     RelationshipID
-- FROM ext_TargetCustomers;

-- COMMAND ----------

--select max(settlementDate) from d0275.readings

-- COMMAND ----------

select count(*) from ext_hh_eligible

-- COMMAND ----------

select count(*) from ext_hh_meterdetails

-- COMMAND ----------

WITH Max_Instance AS (
  SELECT
    r.MPAN,
    e.MPxN_Pk,
    r.measure,
    r.settlementDate,
    r.halfHour,
    MAX(f.fileCreationDate) AS Max_fileCreationDate
  FROM
    d0275.readings AS r
    JOIN d0275.filesread AS f ON r.flowReference = f.flowReference
    JOIN ext_hh_eligible AS e ON r.MPAN = e.ext_ui
    JOIN ext_MigMpan_lcl AS mpanlcl ON mpanlcl.MPAN_Identifier = e.MPxN_PK
   -- JOIN ext_TargetCustomers_lcl AS tc ON mpanlcl.RelationshipID = tc.RelationshipID
  WHERE
    r.settlementDate BETWEEN e.SUPPLY_START_DATE
    AND e.SUPPLY_END_DATE
  GROUP BY
    r.MPAN,
    e.MPxN_Pk,
    r.measure,
    r.settlementDate,
    r.halfHour
) -- Populates HHMeterRead object
INSERT INTO
  mig_hh_meter_read
SELECT 
  DISTINCT CONCAT(
    mi.MPxN_Pk,
    '$',
    r.measure,
    '$',
    date_format(
      r.settlementDate + make_interval(0, 0, 0, 0, 0, r.halfHour * 30, 0),
      'yyyyMMddHHmm'
    )
  ) AS HH_MR_Identifier,
  mi.MPxN_Pk AS MPxN_FK,
  r.measure AS MeterRegisterIdentifier,
  date_format(
      r.settlementDate + make_interval(0, 0, 0, 0, 0, r.halfHour * 30, 0),
      'yyyy-MM-dd HH:mm:ss'
    ) AS ReadingDate,
  r.reading AS Consumption,
  CASE
    WHEN r.measure IN ('AI', 'AE') THEN 'kWh'
    WHEN r.measure IN ('RI', 'RE') THEN 'kVArh'
    ELSE r.measure
  END AS MeterUnit,
  'Normal' AS SequenceType,
  CASE 
    WHEN r.AE = 'E' THEN 'Estimated'
	ELSE 'Normal' 
  END AS ReadingQuality,
  'AMR' AS readingSource,
  'Accepted' AS readingStatus,
  NULL AS RuleFailureID,
  NULL AS RelationshipID,
  0 AS GlideFl,
  0 AS NotMigrateFl
FROM
  d0275.readings AS r
  JOIN d0275.filesread AS f ON r.flowReference = f.flowReference
  JOIN Max_Instance AS mi ON mi.MPAN = r.MPAN
  AND mi.measure = r.measure
  AND mi.settlementDate = r.settlementDate
  AND mi.halfHour = r.halfHour
  AND mi.Max_fileCreationDate = f.fileCreationDate
-- Agreed by Oli K 2022-03-21 to only migrate HH data from 2019-10-01, due to volume of data and speed of processing on a migration attempt.
WHERE date_format(r.settlementDate,'yyyyMMdd') >= '20191001'
-- Uncomment to restrict HHMeterRead output to set list of MPANs
--   AND r.MPAN IN ( '1900091043148', '1580001610626', '1580001583734', '1580001603674', '2700004740685'
--					,'1640000861640', '1900091912485', '1900091912519', '1900091918618', '1050002166087'
--					,'1050002166096', '1050001944669', '1422104200000', '1460000810523', '1170000930946'
--					,'2200042787377', '1014569370475', '1050001438088', '1300035434132', '1425250000000'
--					,'1300035434434', '1460002278570', '1014568904958', '1200061860470', '2100040693633'
--					,'1023468142696', '1640000510168', '1640000811987', '1200010148842', '1023498599529'
--					,'1050000553782', '1470001014134', '2200016895631', '2200030016406', '1014568641835'
--					,'2000027346311', '2200042464273', '2200043028642', '2200041081583', '2000056875894'
--					,'1023506741751', '1429417100002', '1620000509452', '1014573089576', '2700006147230'
--					,'1470001138196', '1470001138201', '2380000325723', '2000054144161', '2380000325714'
--					,'1200010043192', '1200010043208', '1470000401282', '1470000259740', '1470000651523'
--					,'1200010086559', '1200010086568', '1200010086683', '1200010086540', '1200010086692'
--					,'1200061953089', '1200061953070', '1200010147209', '1200010147263', '1200051384286'
--					,'1200010147236' );

-- COMMAND ----------

-- Updates MeterRegisterIdentifier from MigMeterRegister
WITH meter_reg_ids 
AS 
(
  SELECT hhmr.HH_MR_Identifier, MIN(mrc.RegisterIdentifier) AS MinRegisterIdentifier
  FROM ext_hh_meterdetails AS mrc
      JOIN mig_hh_meter_read AS hhmr
        ON mrc.MpxnIdentifier = hhmr.MPxN_FK
        AND mrc.MeterMeasurementType = hhmr.MeterRegisterIdentifier
        AND CAST(hhmr.ReadingDate AS date) BETWEEN mrc.FromDate AND mrc.ToDate
  GROUP BY hhmr.HH_MR_Identifier
)
 
 MERGE INTO mig_hh_meter_read AS hhmr
 USING meter_reg_ids AS mrc
 ON mrc.HH_MR_Identifier = hhmr.HH_MR_Identifier
 WHEN MATCHED
   THEN UPDATE SET MeterRegisterIdentifier = mrc.MinRegisterIdentifier;

-- COMMAND ----------

-- Delete all HH meter read records, where dates within current month (JDM-1419)
DELETE
FROM mig_hh_meter_read
WHERE month(ReadingDate - make_interval(0, 0, 0, 0, 0, 30, 0)) = month(current_date())
  AND year(ReadingDate - make_interval(0, 0, 0, 0, 0, 30, 0)) = year(current_date());

-- COMMAND ----------

-- Delete HH periods for MPANs with HH complex Products from April 2022 onwards
-- These periods will be replaced by D0036s flows, loaded direct to Junifer
DELETE FROM mig_hh_meter_read hhmr
WHERE hhmr.MPxN_FK IN (SELECT MPxN_Pk FROM ext_hh_eligible hhe WHERE hhe.HHTypeFl = 'C')
  AND CAST(ReadingDate - make_interval(0, 0, 0, 0, 0, 30, 0) AS date) >= '2022-04-01';

-- COMMAND ----------

-- Volume of HH reads by month, by HH type
SELECT
  hhe.HHTypeFl
  ,date_format(CAST(hhmr.ReadingDate - make_interval(0, 0, 0, 0, 0, 30, 0) AS date),'yyyy') AS ReadingYear
  ,date_format(CAST(hhmr.ReadingDate - make_interval(0, 0, 0, 0, 0, 30, 0) AS date),'MM') AS ReadingMonth
  ,COUNT(hhmr.MPxN_FK)		AS volbyyear
FROM mig_hh_meter_read hhmr
  LEFT JOIN ext_hh_eligible hhe
    ON hhmr.MPxN_FK = hhe.MPxN_Pk
GROUP BY hhe.HHTypeFl,date_format(CAST(hhmr.ReadingDate - make_interval(0, 0, 0, 0, 0, 30, 0) AS date),'yyyy')
        ,date_format(CAST(hhmr.ReadingDate - make_interval(0, 0, 0, 0, 0, 30, 0) AS date),'MM')
ORDER by HHTypeFl,ReadingYear DESC,ReadingMonth DESC;

-- COMMAND ----------

-- Volume of HH reads by year
SELECT
  date_format(hhmr.ReadingDate,'yyyy') AS ReadingYear
  ,COUNT(hhmr.MPxN_FK)		AS volbyyear
FROM mig_hh_meter_read hhmr
GROUP BY date_format(hhmr.ReadingDate,'yyyy')
ORDER by ReadingYear;

-- COMMAND ----------

select count(*) from mig_hh_meter_read;

-- COMMAND ----------

DELETE FROM mig_hh_meter_read hhmr
WHERE hhmr.MeterRegisterIdentifier IN ('AI','AE','RI','RE');

-- COMMAND ----------

select count(*) from mig_hh_meter_read;

-- COMMAND ----------

CREATE 
OR REPLACE TABLE mig_HH_D0036s_lcl
(
MPAN string NOT NULL,
ExtractFrom Date NOT NULL
) USING DELTA LOCATION 's3a://eco-datalake-production/sandbox/junifer-data-migration/mig-hh-D0036s-lcl'

-- COMMAND ----------

INSERT INTO mig_HH_D0036s_lcl
-- Generate file of MPANs for complex HHs, indicating where D0036 generation is required from 01/04/2022 onward
SELECT DISTINCT
    ext_ui as MPAN
    ,'2022-04-01' as ExportFrom
FROM ext_HH_Eligible hhe
WHERE hhe.HHTypeFl = 'C'
  AND CAST(SUPPLY_END_DATE AS date) >= '2022-04-01'
UNION
-- Generate file of MPANs for simple HHs, indicating where D0036 generation is required from migration month onward
SELECT ext_ui as MPAN
    ,date_add(last_day(add_months(current_date(), -1)),1)as ExportFrom
FROM ext_HH_Eligible hhe
WHERE hhe.HHTypeFl = 'S'
  AND month(SUPPLY_END_DATE) >= month(current_date())
  AND year(SUPPLY_END_DATE) >= year(current_date());


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from ecotricity_boto3_utils.s3 import remove_keys_in_directory
-- MAGIC from ecotricity_boto3_utils.session import get_global_boto3_session
-- MAGIC from ecotricity_boto3_utils.ssm import get_datalake
-- MAGIC 
-- MAGIC MIGRATION_HH_TEXT = "sandbox/migration-hh-text/D0036s"
-- MAGIC 
-- MAGIC remove_keys_in_directory(get_global_boto3_session(), get_datalake(), f"{MIGRATION_HH_TEXT}/")
-- MAGIC mig_hh_df = spark.sql(f"SELECT * FROM mig_HH_D0036s_lcl")
-- MAGIC 
-- MAGIC (
-- MAGIC     mig_hh_df.drop(
-- MAGIC 
-- MAGIC     ).write.format("csv")
-- MAGIC     .option("header", False)
-- MAGIC     .option("delimiter", "|")
-- MAGIC     .save(f"s3a://{get_datalake()}/{MIGRATION_HH_TEXT}")
-- MAGIC  )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from ecotricity_boto3_utils.s3 import remove_keys_in_directory
-- MAGIC from ecotricity_boto3_utils.session import get_global_boto3_session
-- MAGIC from ecotricity_boto3_utils.ssm import get_datalake
-- MAGIC 
-- MAGIC MIGRATION_HH_TEXT = "sandbox/migration-hh-text/HHMeterRead"
-- MAGIC 
-- MAGIC remove_keys_in_directory(get_global_boto3_session(), get_datalake(), f"{MIGRATION_HH_TEXT}/")
-- MAGIC mig_hh_df = spark.sql(f"SELECT * FROM mig_hh_meter_read")
-- MAGIC 
-- MAGIC (
-- MAGIC     mig_hh_df.drop(
-- MAGIC         "RuleFailureID",
-- MAGIC         "RelationshipID",
-- MAGIC         "GlideFl",
-- MAGIC         "NotMigrateFl"
-- MAGIC     ).write.format("csv")
-- MAGIC     .option("header", False)
-- MAGIC     .option("delimiter", "|")
-- MAGIC     .save(f"s3a://{get_datalake()}/{MIGRATION_HH_TEXT}")
-- MAGIC  )

-- COMMAND ----------

--%python
-- from ecotricity_boto3_utils.s3 import remove_keys_in_directory
-- from ecotricity_boto3_utils.session import get_global_boto3_session
-- from ecotricity_boto3_utils.ssm import get_datalake

-- MIGRATION_HH_TEXT = "sandbox/migration-hh-text/HHMeterReadSingleFile"

-- remove_keys_in_directory(get_global_boto3_session(), get_datalake(), f"{MIGRATION_HH_TEXT}/")
-- mig_hh_df = spark.sql(f"SELECT * FROM mig_hh_meter_read").coalesce(1)

-- (
--     mig_hh_df.drop(
--         "RuleFailureID",
--         "RelationshipID",
--         "GlideFl",
--         "NotMigrateFl"
--     ).write.format("csv")
--     .option("header", False)
--     .option("delimiter", "|")
--     .option("encoding", "UTF-8")
--     .save(f"s3a://{get_datalake()}/{MIGRATION_HH_TEXT}")
-- )

-- COMMAND ----------

--%python
--output_file_path = [x.path for x in dbutils.fs.ls(f"s3a://{get_datalake()}/{MIGRATION_HH_TEXT}") if x.name[-4:] == '.csv'][0]
--dest_file_path = ''
--
--dbutils.fs.cp(output_file_path, dest_file_path)

-- COMMAND ----------

--%python
--from ecotricity_boto3_utils.ssm import get_datalake
--get_datalake()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # from datetime import datetime, date
-- MAGIC 
-- MAGIC # SQL_SERVER = "172.16.144.17"
-- MAGIC # SQL_DATABASE = "DataMigrationLive"
-- MAGIC # schema = "ext"
-- MAGIC # table_name = "mighhmeterread"
-- MAGIC # SQL_USERNAME = "databricks"
-- MAGIC # SQL_PASSWORD = "oVeYkwC80WOE"
-- MAGIC # CURRENT_DATE = date.today()
-- MAGIC 
-- MAGIC 
-- MAGIC # def get_dttime():
-- MAGIC #   """Return the datetime at which the function is called."""
-- MAGIC   
-- MAGIC #   return datetime.now()
-- MAGIC 
-- MAGIC 
-- MAGIC # break_loop = False
-- MAGIC # first_iteration = True
-- MAGIC # for target_year in range(2017, CURRENT_DATE.year + 1):
-- MAGIC #     if first_iteration:
-- MAGIC #         try:
-- MAGIC #             df = spark.sql(f"SELECT * FROM mig_hh_meter_read WHERE YEAR(ReadingDate) = '{target_year}'")
-- MAGIC #             print(f"{get_dttime()} Initial truncation of the mig_hh_meter_read table is beginning.")
-- MAGIC #             (
-- MAGIC #                 df.write.option("truncate", "true")
-- MAGIC #                 .format("com.microsoft.sqlserver.jdbc.spark")
-- MAGIC #                 .mode("overwrite")
-- MAGIC #                 .option("url", f"jdbc:sqlserver://{SQL_SERVER};databaseName={SQL_DATABASE};")
-- MAGIC #                 .option("dbtable", f"{schema}.{table_name}")
-- MAGIC #                 .option("user", SQL_USERNAME)
-- MAGIC #                 .option("password", SQL_PASSWORD)
-- MAGIC #                 .option("batchsize", "40000")
-- MAGIC #                 .save()
-- MAGIC #             )
-- MAGIC #             print(f"{get_dttime()} Truncation/write for mig_hh_meter_read {target_year} has completed.")
-- MAGIC #         except ValueError as error :
-- MAGIC #             print(f"{get_dttime()} Connector write failed", error)
-- MAGIC #         first_iteration=False
-- MAGIC #         continue
-- MAGIC #     # for anything other than the first iteration
-- MAGIC #     for month in range(1, 13):
-- MAGIC #         if target_year == CURRENT_DATE.year and month > CURRENT_DATE.month:
-- MAGIC #             print(f"{get_dttime()} Now in the future ({target_year}-{month}), halting.")
-- MAGIC #             break_loop = True
-- MAGIC #             break
-- MAGIC #         else:
-- MAGIC #             month_df = spark.sql(
-- MAGIC #                 f"""
-- MAGIC #             SELECT *
-- MAGIC #               FROM mig_hh_meter_read
-- MAGIC #              WHERE YEAR(ReadingDate) = '{target_year}'
-- MAGIC #                AND MONTH(ReadingDate) = '{month}'
-- MAGIC #             """
-- MAGIC #             ) df.where(f"MONTH(ReadingDate) = '{month}'"
-- MAGIC #             try:
-- MAGIC #                 print(f"{get_dttime()} Writing table to SQL Server for reads in {target_year}-{month}")
-- MAGIC #                 (
-- MAGIC #                     month_df.write.format("com.microsoft.sqlserver.jdbc.spark")
-- MAGIC #                     .mode("append")
-- MAGIC #                     .option("url", f"jdbc:sqlserver://{SQL_SERVER};databaseName={SQL_DATABASE};")
-- MAGIC #                     .option("dbtable", f"{schema}.{table_name}")
-- MAGIC #                     .option("user", SQL_USERNAME)
-- MAGIC #                     .option("password", SQL_PASSWORD)
-- MAGIC #                     .option("batchsize", "40000")
-- MAGIC #                     .save()
-- MAGIC #                 )
-- MAGIC #                 print(f"{get_dttime()} Completed write for table for reads in {target_year}-{month}")
-- MAGIC #             except ValueError as error :
-- MAGIC #                 print(f"{get_dttime()} Connector write failed", error)
-- MAGIC #     if break_loop:
-- MAGIC #         break
