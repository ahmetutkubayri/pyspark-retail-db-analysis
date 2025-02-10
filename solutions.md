# Solutions to Retail Database Analysis

This file contains solutions to the given business questions using PySpark.

## Q-1: Count Distinct Order Items
```python
distinct_order_ids = order_items.select("orderItemOrderId").distinct().count()
print(f"Distinct OrderItemOrderId count: {distinct_order_ids}")
```

## Q-2: Row Counts for Orders and Order Items
```python
orders_count = orders.count()
order_items_count = order_items.count()
print(f"Orders: {orders_count}, Order Items: {order_items_count}")
```

## Q-3 & Q-4: Most Canceled Products & Categories by Sales (Stored in Parquet)
```python
canceled_sales = orders.join(order_items, orders.orderId == order_items.orderItemOrderId)    .filter(orders.orderStatus == "CANCELED")    .groupBy("productName")    .sum("orderItemSubTotal")    .orderBy("sum(orderItemSubTotal)", ascending=False)

canceled_sales.write.mode("overwrite").parquet("canceled_products.parquet")
```

## Q-5 & Q-6: Best Sales Month & Day (Turkish)
```python
from pyspark.sql.functions import date_format

sales_by_month = orders.withColumn("MonthYear", date_format("orderDate", "MMMM yyyy"))    .groupBy("MonthYear").sum("orderItemSubTotal")    .orderBy("sum(orderItemSubTotal)", ascending=False)

sales_by_day = orders.withColumn("Weekday", date_format("orderDate", "EEEE"))    .groupBy("Weekday").sum("orderItemSubTotal")    .orderBy("sum(orderItemSubTotal)", ascending=False)
```

## Q-7: Full Table Join & Store in PostgreSQL
```python
full_table = orders.join(order_items, "orderId")    .join(customers, "customerId")    .join(products, "productId")    .join(categories, "categoryId")    .join(departments, "departmentId")

full_table.write    .format("jdbc")    .option("url", "jdbc:postgresql://localhost/test1")    .option("dbtable", "retail_all")    .option("user", "your_user")    .option("password", "your_password")    .save()
```
