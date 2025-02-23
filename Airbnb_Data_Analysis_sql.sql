-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### SQL for acquring insights from the data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Listing Analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Total number of listings**

-- COMMAND ----------

SELECT COUNT(*) AS total_listings
FROM delta.`/FileStore/tables/airbnb_delta`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Average price of Listings**

-- COMMAND ----------

SELECT round(AVG(price),2) AS average_price
FROM delta.`/FileStore/tables/airbnb_delta`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Number of listings by neighbourhood**

-- COMMAND ----------

SELECT neighbourhood, COUNT(*) AS listing_count
FROM delta.`/FileStore/tables/airbnb_delta`
GROUP BY neighbourhood
ORDER BY listing_count DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Price Analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Top 4 expensive listings**

-- COMMAND ----------

SELECT name, neighbourhood, price
FROM delta.`/FileStore/tables/airbnb_delta`
ORDER BY price DESC
LIMIT 4;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Average price by Room type**

-- COMMAND ----------

SELECT room_type,round(AVG(price)) as avg_price
FROM delta.`/FileStore/tables/airbnb_delta`
GROUP BY room_type
ORDER BY avg_price desc LIMIT 4;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Host Analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Average price by host**

-- COMMAND ----------

SELECT host_id, round(AVG(price),2) as avg_price
FROM delta.`/FileStore/tables/airbnb_delta`
GROUP BY host_id
ORDER BY avg_price DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Occupancy and Availability Analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Top 5 Listings with High prices but Low reviews**

-- COMMAND ----------

SELECT name, neighbourhood, price, number_of_reviews
FROM delta.`/FileStore/tables/airbnb_delta`
WHERE price > (SELECT AVG(price) FROM delta.`/FileStore/tables/airbnb_delta`)
  AND number_of_reviews < (SELECT AVG(number_of_reviews) FROM delta.`/FileStore/tables/airbnb_delta`)
ORDER BY price DESC LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Most Popular Room Types in Each Neighborhood**

-- COMMAND ----------

SELECT neighbourhood, room_type, count(*) listing_count
FROM  delta.`/FileStore/tables/airbnb_delta`
where room_type IS NOT NULL
GROUP BY neighbourhood, room_type
ORDER BY listing_count DESC LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Correlation Analysis

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Correlation Between Price and Number of Reviews**

-- COMMAND ----------

SELECT round(CORR(price, number_of_reviews),3) AS price_review_correlation
FROM delta.`/FileStore/tables/airbnb_delta`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC It indicates that `price` doesn't significantly change over time.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Correlation Between Price and Minimum Nights**

-- COMMAND ----------

SELECT round(CORR(price, minimum_nights),3) AS price_min_nights_correlation
FROM delta.`/FileStore/tables/airbnb_delta`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC It indicates that there is no relationship between `price`  and  `minimum_nights`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Save Results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Saving results to a CSV file**

-- COMMAND ----------

INSERT OVERWRITE DIRECTORY 'dbfs:/FileStore/results/high_price_listings'
USING csv
SELECT name, neighbourhood, price
FROM delta.`/FileStore/tables/airbnb_delta`
