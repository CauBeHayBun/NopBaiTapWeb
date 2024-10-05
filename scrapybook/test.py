from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# Bước 1: Tạo SparkConf
conf = SparkConf() \
    .setAppName("MongoDB_PySpark_Cleaning") \
    .setMaster("local[*]") \
    .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.0.1") \
    .set("spark.mongodb.input.uri", "mongodb://localhost:27017/myscrapybook_db.books") \
    .set("spark.mongodb.output.uri", "mongodb://localhost:27017/myscrapybook_db.books")

# Bước 2: Tạo SparkContext
sc = SparkContext(conf=conf)

# Bước 3: Tạo SparkSession từ SparkContext
spark = SparkSession(sc)

print("SparkSession đã được tạo.")

# Đọc dữ liệu từ MongoDB
df = spark.read.format("mongo").load()
print("Đã đọc dữ liệu từ MongoDB.")

# Kiểm tra số lượng hàng trong DataFrame
num_rows = df.count()
print(f"Số lượng hàng: {num_rows}")

# Hiển thị dữ liệu nếu có
if num_rows > 0:
    df.show()  # Hiển thị 20 hàng đầu tiên
    print("Dữ liệu trong DataFrame:")
    pandas_df = df.toPandas()
    print(pandas_df)
else:
    print("Không có dữ liệu để hiển thị.")

# Dừng SparkContext khi không cần thiết
sc.stop()
