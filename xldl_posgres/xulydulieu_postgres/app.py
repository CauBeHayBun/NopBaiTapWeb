import pandas as pd
import psycopg2
from pymongo import MongoClient
from sqlalchemy import create_engine

# Kết nối đến MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['myscrapybook_db']
collection = db['books']

# Lấy dữ liệu từ MongoDB
data = list(collection.find({}))
df = pd.DataFrame(data)

# Xử lý dữ liệu
df['noidung_brand'] = df['noidung_brand'].str.replace('Mua sách online tại Bookbuy.vn và nhận nhiều ưu đãi.', '', regex=False)
df['giahientai'] = df['giahientai'].str.replace(r'[^\d]', '', regex=True)
df['giathitruong'] = df['giathitruong'].str.replace(r'[^\d]', '', regex=True)
df['sotrang'] = df['sotrang'].str.replace(r'[^\d]', '', regex=True)
df['trongluong'] = df['trongluong'].str.replace(r'[^\d]', '', regex=True)
# Xóa khoảng trắng đầu cuối cho tất cả các cột kiểu chuỗi
string_columns = df.select_dtypes(include=['object']).columns  # Lấy danh sách các cột kiểu chuỗi

for col in string_columns:
    df[col] = df[col].str.strip()  # Dùng str.strip để xóa khoảng trắng đầu cuối

# Chuyển đổi kiểu dữ liệu
df['coursename'] = df['coursename'].astype(str)
df['tacgia'] = df['tacgia'].astype(str)
df['tietkiem'] = pd.to_numeric(df['tietkiem'].replace('', None), errors='coerce').fillna(0).astype(int)
df['giahientai'] = pd.to_numeric(df['giahientai'].replace('', None), errors='coerce').fillna(0).astype(int)
df['giathitruong'] = pd.to_numeric(df['giathitruong'].replace('', None), errors='coerce').fillna(0).astype(int)
df['tinhtrang'] = df['tinhtrang'].astype(str)
df['dichgia'] = df['dichgia'].replace('', 'không có').fillna('không có').astype(str)
df['nhaxuatban'] = df['nhaxuatban'].astype(str)
df['sotrang'] = pd.to_numeric(df['sotrang'].replace('', None), errors='coerce').fillna(0).astype(int)
df['trongluong'] = pd.to_numeric(df['trongluong'].replace('', None), errors='coerce').fillna(0).astype(int)
df['noidung_brand'] = df['noidung_brand'].astype(str)

# Kết nối tới PostgreSQL
conn = psycopg2.connect(
    dbname='dbmyscrapybook',
    user='postgres',
    password='123',
    host='localhost',
    port='5432',
    options='-c client_encoding=UTF8'
)
cursor = conn.cursor()

# Tạo bảng
cursor.execute('''
CREATE TABLE IF NOT EXISTS authors (
    authors_id SERIAL PRIMARY KEY,
    tacgia VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS publishers (
    publisher_id SERIAL PRIMARY KEY,
    nhaxuatban VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS bookdetails (
    bookdetails_id SERIAL PRIMARY KEY,
    sotrang INT,
    trongluong INT,
    noidung_brand TEXT,
    dichgia VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS pricing (
    pricing_id SERIAL PRIMARY KEY,
    giahientai INT,
    giathitruong INT,
    tietkiem INT
);

CREATE TABLE IF NOT EXISTS books (
    book_id SERIAL PRIMARY KEY,
    coursename VARCHAR(255),
    publisher_id INT REFERENCES publishers(publisher_id),
    pricing_id INT REFERENCES pricing(pricing_id),
    bookdetails_id INT REFERENCES bookdetails(bookdetails_id),
    authors_id INT REFERENCES authors(authors_id)
);
''')

# Commit changes to create tables
conn.commit()

# Chèn dữ liệu vào các bảng
for index, row in df.iterrows():
    cursor.execute('INSERT INTO publishers (nhaxuatban) VALUES (%s) RETURNING publisher_id', (row['nhaxuatban'],))
    publisher_id = cursor.fetchone()[0]
    
    cursor.execute('INSERT INTO authors (tacgia) VALUES (%s) RETURNING authors_id', (row['tacgia'],))
    authors_id = cursor.fetchone()[0]
    
    cursor.execute('''
        INSERT INTO bookdetails (sotrang, trongluong, noidung_brand, dichgia)
        VALUES (%s, %s, %s, %s) RETURNING bookdetails_id
    ''', (row['sotrang'], row['trongluong'], row['noidung_brand'], row['dichgia']))
    bookdetails_id = cursor.fetchone()[0]
    
    cursor.execute('''
        INSERT INTO pricing (giahientai, giathitruong, tietkiem)
        VALUES (%s, %s, %s) RETURNING pricing_id
    ''', (row['giahientai'], row['giathitruong'], row['tietkiem']))
    pricing_id = cursor.fetchone()[0]
    
    cursor.execute('''
        INSERT INTO books (coursename, publisher_id, pricing_id, bookdetails_id, authors_id)
        VALUES (%s, %s, %s, %s, %s)
    ''', (row['coursename'], publisher_id, pricing_id, bookdetails_id, authors_id))

# Commit changes for inserting data
conn.commit()

# Đóng kết nối
cursor.close()
conn.close()
