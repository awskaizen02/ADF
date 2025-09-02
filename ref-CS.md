Data Ingestion
# SQL
```
-- Reading from CSV
SELECT * FROM READ_CSV('path/to/file.csv');

-- Reading from database
SELECT * FROM source_table;

```

# import pandas as pd
```
# From CSV
df = pd.read_csv('path/to/file.csv')

# From database
import sqlalchemy
engine = sqlalchemy.create_engine('database_connection_string')
df = pd.read_sql('SELECT * FROM source_table', engine)
```
# PySpark

```
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Ingestion').getOrCreate()

# From CSV
df = spark.read.csv('path/to/file.csv', header=True, inferSchema=True)

# From database
df = spark.read.format('jdbc') \
    .option('url', 'jdbc:database_connection_string') \
    .option('dbtable', 'source_table') \
    .load()
```

# Spark SQL
```
-- From CSV
CREATE TEMPORARY VIEW temp_table
USING csv
OPTIONS (path 'path/to/file.csv', header 'true', inferSchema 'true');

-- From database
CREATE TEMPORARY VIEW temp_table
USING jdbc
OPTIONS (url 'jdbc:database_connection_string', 
         dbtable 'source_table');
```

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Data Transformation

-- Filtering and aggregation
SELECT 
    category,
    COUNT(*) as item_count,
    AVG(price) as avg_price
FROM products
WHERE price > 10
GROUP BY category
HAVING COUNT(*) > 5;

# Filtering and aggregation
filtered = df[df['price'] > 10]
result = filtered.groupby('category').agg(
    item_count=('price', 'count'),
    avg_price=('price', 'mean')
).reset_index()

# Joining
merged = pd.merge(df1, df2, on='key_column', how='inner')

# Filtering and aggregation
from pyspark.sql.functions import avg, count

result = df.filter(df.price > 10) \
    .groupBy('category') \
    .agg(count('*').alias('item_count'),
         avg('price').alias('avg_price')) \
    .filter(count('*') > 5)

# Joining
merged = df1.join(df2, on='key_column', how='inner')


-- Register DataFrame as temp view
df.createOrReplaceTempView('products')

-- Query
SELECT 
    category,
    COUNT(*) as item_count,
    AVG(price) as avg_price
FROM products
WHERE price > 10
GROUP BY category
HAVING COUNT(*) > 5;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Optimization Techniques



-- Create indexes for faster queries
CREATE INDEX idx_category ON products(category);

-- Materialized views for expensive aggregations
CREATE MATERIALIZED VIEW category_summary AS
SELECT category, AVG(price) as avg_price
FROM products
GROUP BY category;

-- Partitioning
CREATE TABLE partitioned_table (id INT, value STRING, date DATE)
PARTITIONED BY (date);


# Reduce memory usage
df['column'] = df['column'].astype('category')  # For low-cardinality strings
df['numeric_col'] = pd.to_numeric(df['numeric_col'], downcast='float')

# Use efficient data types
dtypes = {
    'id': 'int32',
    'value': 'category',
    'date': 'datetime64[ns]'
}
df = df.astype(dtypes)

# Vectorized operations (faster than loops)
df['new_col'] = df['col1'] * df['col2']  # Vectorized


# Partitioning for parallel processing
df.repartition(100, 'category')  # 100 partitions by category

# Caching frequently used DataFrames
df.cache()  # Or persist(StorageLevel.MEMORY_AND_DISK)

# Broadcast join for small tables
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), 'key')

# Predicate pushdown
df.filter(df.date > '2023-01-01')  # Pushes filter to data source


-- Partitioning
CREATE TABLE partitioned_table (id INT, value STRING)
PARTITIONED BY (date DATE)
STORED AS PARQUET;

-- Bucketing for frequent joins
CREATE TABLE bucketed_table (id INT, value STRING)
CLUSTERED BY (id) INTO 10 BUCKETS;

-- Use optimized file formats
CREATE TABLE parquet_table
STORED AS PARQUET
AS SELECT * FROM source_table;

-- Configure Spark settings
SET spark.sql.shuffle.partitions=200;
SET spark.sql.inMemoryColumnarStorage.compressed=true;

-- Ingestion
CREATE TABLE staging AS
SELECT * FROM READ_CSV('data.csv');

-- Transformation
CREATE TABLE transformed AS
SELECT 
    category,
    COUNT(*) as item_count,
    AVG(price) as avg_price
FROM staging
WHERE price > 10
GROUP BY category;

-- Optimization
CREATE INDEX idx_category ON transformed(category);
CREATE MATERIALIZED VIEW summary AS
SELECT * FROM transformed;


# Ingestion
df = pd.read_csv('data.csv')

# Transformation
result = (df[df['price'] > 10]
          .groupby('category')
          .agg(item_count=('price', 'count'),
               avg_price=('price', 'mean'))
          .reset_index())

# Optimization
result['category'] = result['category'].astype('category')
result.to_parquet('output.parquet', compression='snappy')


# Ingestion
df = spark.read.csv('data.csv', header=True, inferSchema=True)

# Transformation
from pyspark.sql.functions import avg, count

result = (df.filter(df.price > 10)
          .groupBy('category')
          .agg(count('*').alias('item_count'),
               avg('price').alias('avg_price')))

# Optimization
result.repartition(10, 'category').write.parquet('output', mode='overwrite')

## Spark SQL Pipeline

-- Ingestion
CREATE TEMPORARY VIEW data
USING csv
OPTIONS (path 'data.csv', header 'true', inferSchema 'true');

-- Transformation
CREATE TEMPORARY VIEW transformed AS
SELECT 
    category,
    COUNT(*) as item_count,
    AVG(price) as avg_price
FROM data
WHERE price > 10
GROUP BY category;

-- Optimization
CREATE TABLE optimized
USING parquet
PARTITIONED BY (category)
AS SELECT * FROM transformed;


--------------------------------------------------------------------------------------------------------------|
Operation                SQL                Python                PySpark        Spark                        |
--------------------------------------------------------------------------------------------------------------|
                                                                                                              |
Read CSV              READ_CSV()       pd.read_csv()        spark.read.csv()    USING csv                     |
Filter                WHERE            df[df.col > x]       filter(df.col > x)   WHERE                        |
GroupBy               GROUP BY         groupby().agg()      groupBy().agg()      GROUP BY                     |
Optimize              Indexes          astype()             repartition()        PARTITIONED BY               |
Output                CREATE TABLE     to_parquet()         write.parquet()      CREATE TABLE ... STORED AS   |
--------------------------------------------------------------------------------------------------------------|
