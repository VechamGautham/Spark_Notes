from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Agg Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")

    invoice_df.select(f.count("*").alias("Count *"),
                      f.sum("Quantity").alias("TotalQuantity"),
                      f.avg("UnitPrice").alias("AvgPrice"),
                      f.countDistinct("InvoiceNo").alias("CountDistinct")
                      ).show()

    invoice_df.selectExpr(
        "count(1) as `count 1`",
        "count(StockCode) as `count field`",
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as AvgPrice"
    ).show()

    invoice_df.createOrReplaceTempView("invoice_df")
    summary_sql = spark.sql("""
          SELECT Country, InvoiceNo,
                sum(Quantity) as TotalQuantity,
                round(sum(Quantity*UnitPrice),2) as InvoiceValue
          FROM sales
          GROUP BY Country, InvoiceNo""")

    summary_sql.show()

    summary_df = invoice_df \
        .groupBy("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"),
             f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValueExpr")
             )

    summary_df.show() 

    summary_sql_2 = spark.sql("""
        select Country ,
        weekofyear(to_timestamp(InvoiceDate, 'dd-MM-yyyy H.mm')) as WeekNumber ,
        count(distinct invoiceno) as numinvoices,
        sum(Quantity) as totalquantity,
        round(sum(Quantity * UnitPrice),2) as InvoiceValue 
        from invoice_df 
        group by Country , WeekNumber 
        order by Country, WeekNumber
    """).show()

    summary_df_2 = invoice_df \
                    .withColumn("WeekNumber",f.weekofyear(f.to_timestamp("InvoiceDate","dd-MM-yyyy H.mm"))) \
                    .groupBy("Country","WeekNumber") \
                    .agg(
                        f.count_distinct("InvoiceNo").alias("Numinvoices"),
                        f.sum("Quantity").alias("sum"),
                        f.round(f.sum(f.col("quantity")*f.col("unitprice")),2).alias("InvoiceValue")
                    ) \
                    .orderBy("Country","WeekNumber")
    summary_df_2.show()
