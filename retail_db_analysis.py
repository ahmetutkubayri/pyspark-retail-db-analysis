from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("RetailDBAnalysis").master("local[2]").getOrCreate()

orders_df = spark.read.csv("/opt/examples/orders.csv", header=True, inferSchema=True)
order_items_df = spark.read.csv("/opt/examples/order_items.csv", header=True, inferSchema=True)
products_df = spark.read.csv("/opt/examples/products.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("/opt/examples/customers.csv", header=True, inferSchema=True)
categories_df = spark.read.csv("/opt/examples/categories.csv", header=True, inferSchema=True)
departments_df = spark.read.csv("/opt/examples/departments.csv", header=True, inferSchema=True)

distinct_order_ids = order_items_df.select("orderItemOrderId").distinct().count()
print("Distinct orderItemOrderId count:", distinct_order_ids)


orders_count = orders_df.count()
order_items_count = order_items_df.count()
print("Orders row count:", orders_count)
print("Order Items row count:", order_items_count)

canceled_orders = orders_df.filter(orders_df.orderStatus == "CANCELED")


canceled_orders.show(10)

canceled_product_sales = canceled_orders \
    .join(order_items_df, canceled_orders.orderId == order_items_df.orderItemOrderId) \
    .join(products_df, order_items_df.orderItemProductId == products_df.productId) \
    .groupBy("productName") \
    .agg(F.round(F.sum("orderItemSubTotal"), 2).alias("total_sales")) \
    .orderBy(F.desc("total_sales"))

canceled_product_sales.show(10)

canceled_product_sales.write \
.mode("overwrite") \
.format("parquet") \
.save("output/most_canceled_products.parquet")


canceled_order_items = order_items_df.join(canceled_orders, order_items_df.orderItemOrderId == canceled_orders.orderId)

canceled_categories_sales = canceled_order_items \
    .join(products_df, canceled_order_items.orderItemProductId == products_df.productId) \
    .join(categories_df, products_df.productCategoryId == categories_df.categoryId) \
    .groupBy("categoryName") \
    .agg(F.round(F.sum("orderItemSubTotal"),2).alias("total_sales")) \
    .orderBy(F.desc("total_sales"))

canceled_categories_sales.show(10)

monthly_sales = orders_df \
    .join(order_items_df, orders_df.orderId == order_items_df.orderItemOrderId) \
    .withColumn("month_year", F.date_format("orderDate", "MMMM-yyyy")) \
    .groupBy("month_year") \
    .agg(F.round(F.sum("orderItemSubTotal"), 2).alias("total_sales")) \
    .orderBy(F.desc("total_sales"))

months_tr = {
    "January": "Ocak", "February": "Şubat", "March": "Mart", "April": "Nisan",
    "May": "Mayıs", "June": "Haziran", "July": "Temmuz", "August": "Ağustos",
    "September": "Eylül", "October": "Ekim", "November": "Kasım", "December": "Aralık"
}

monthly_sales = orders_df \
    .join(order_items_df, orders_df["orderId"] == order_items_df["orderItemOrderId"]) \
    .withColumn("month", F.date_format("orderDate", "MMMM")) \
    .withColumn("year", F.year("orderDate")) \
    .groupBy("month", "year") \
    .agg(F.round(F.sum("orderItemSubTotal"), 2).alias("total_sales")) \
    .orderBy(F.desc("total_sales"))

top_monthly_sales = monthly_sales.limit(1).collect()[0]

month_tr = months_tr[top_monthly_sales["month"]]
year = top_monthly_sales["year"]

output = f"{year} yılının {month_tr} ayında en çok satış olmuş"
print(output)

days_tr = {
    "Monday": "Pazartesi", "Tuesday": "Salı", "Wednesday": "Çarşamba",
    "Thursday": "Perşembe", "Friday": "Cuma", "Saturday": "Cumartesi", "Sunday": "Pazar"
}

weekly_sales = orders_df \
    .join(order_items_df, orders_df["orderId"] == order_items_df["orderItemOrderId"]) \
    .withColumn("day_of_week", F.date_format("orderDate", "EEEE")) \
    .groupBy("day_of_week") \
    .agg(F.round(F.sum("orderItemSubTotal"), 2).alias("total_sales")) \
    .orderBy(F.desc("total_sales"))

top_day_sales = weekly_sales.limit(1).collect()[0]

# Convert the English day name to Turkish
day_of_week_tr = days_tr[top_day_sales["day_of_week"]]

# Format the output in the requested format
output = f"En çok satış yapılan gün: {day_of_week_tr}"
print(output)


retail_all = orders_df \
    .join(order_items_df, orders_df.orderId == order_items_df.orderItemOrderId, "left") \
    .join(customers_df, orders_df.orderCustomerId == customers_df.customerId, "left") \
    .join(products_df, order_items_df.orderItemProductId == products_df.productId, "left") \
    .join(categories_df, products_df.productCategoryId == categories_df.categoryId, "left") \
    .join(departments_df, categories_df.categoryDepartmentId == departments_df.departmentId, "left") \
    .select(
        F.col("orderItemName"),
        F.col("orderItemOrderId"),
        F.col("orderItemProductId"),
        F.col("orderItemQuantity"),
        F.col("orderItemSubTotal"),
        F.col("orderItemProductPrice"),
        F.col("orderId"),
        F.col("orderDate").cast("timestamp"),
        F.col("orderCustomerId"),
        F.col("orderStatus"),
        F.col("customerId"),
        F.col("customerFName"),
        F.col("customerLName"),
        F.col("customerEmail"),
        F.col("customerPassword"),
        F.col("customerStreet"),
        F.col("customerCity"),
        F.col("customerState"),
        F.col("customerZipcode"),
        F.col("productId"),
        F.col("productCategoryId"),
        F.col("productName"),
        F.col("productDescription"),
        F.col("productPrice"),
        F.col("productImage"),
        F.col("categoryId"),
        F.col("categoryDepartmentId"),
        F.col("categoryName"),
        F.col("departmentId"),
        F.col("departmentName")
    )

retail_all.printSchema()

db_ip = "172.19.0.2"
user_name = "postgres"
password = "Ankara06"

jdbcUrl = f"jdbc:postgresql://{db_ip}:5432/test1?user={user_name}&password={password}"

retail_all.write.jdbc(
    url=jdbcUrl,
    table="churn_spark",
    mode="overwrite",
    properties={"driver": "org.postgresql.Driver"}
)

