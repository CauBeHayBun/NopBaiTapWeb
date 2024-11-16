import pandas as pd
import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import re
import time


# Kết nối Kafka Consumer
consumer = Consumer({
    'bootstrap.servers': 'kafka_container:29092',  # Địa chỉ Kafka broker
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['bookdata'])  # Thay 'bookdata' bằng tên topic bạn muốn nhận dữ liệu
# Hàm để xử lý dữ liệu nhận từ Kafka
def consume_data():
    print("Bắt đầu nhận dữ liệu từ Kafka...")
    
    # Cấu hình Consumer
    consumer = Consumer({
        'bootstrap.servers': 'kafka_container:29092',
        'group.id': 'mygroup' + str(time.time()),  # Thêm timestamp để tạo group ID unique
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'session.timeout.ms': 60000,
        'max.poll.interval.ms': 600000
    })

    print("Đã tạo consumer, đang subscribe vào topic...")
    consumer.subscribe(['bookdata'])
    
    data = []
    messages_count = 0
    start_time = time.time()
    timeout = 20  # Tăng timeout lên 60 giây
    
    try:
        while True:
            if time.time() - start_time > timeout:
                print(f"Đã hết thời gian chờ ({timeout} giây)")
                break
                
            print("Đang đợi message...")
            msg = consumer.poll(5.0)  # Tăng thời gian poll lên 5 giây
            
            if msg is None:
                print("Không nhận được message, đang thử lại...")
                time.sleep(1)  # Thêm delay
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Đã đọc hết partition {msg.partition()}")
                    break
                else:
                    print(f"Lỗi Kafka: {msg.error()}")
                    continue
            
            try:
                value = json.loads(msg.value().decode('utf-8'))
                data.append(value)
                messages_count += 1
                print(f"Đã nhận message {messages_count}: {value.get('coursename', 'Unknown')}")
                
            except Exception as e:
                print(f"Lỗi khi xử lý message: {e}")
                
    except Exception as e:
        print(f"Lỗi trong quá trình nhận message: {e}")
    finally:
        print("Đóng consumer...")
        consumer.close()
    
    print(f"Tổng số message đã nhận: {len(data)}")
    return data

# Lấy dữ liệu từ Kafka
data = consume_data()
print(f"Số dữ liệu lấy về từ Kafka: {len(data)}")  # In ra số dữ liệu lấy về từ Kafka


if not data:
    print("No data received from Kafka")
else:
    df = pd.DataFrame(data)

    # Tiến hành xử lý dữ liệu như trước
    df['noidung_brand'] = df['noidung_brand'].str.replace('Mua sách online tại Bookbuy.vn và nhận nhiều ưu đãi.', '', regex=False)
    df['giahientai'] = df['giahientai'].str.replace(r'[^\d]', '', regex=True)
    df['giathitruong'] = df['giathitruong'].str.replace(r'[^\d]', '', regex=True)
    df['sotrang'] = df['sotrang'].str.replace(r'[^\d]', '', regex=True)
    df['trongluong'] = df['trongluong'].str.replace(r'[^\d]', '', regex=True)

    # Xóa khoảng trắng đầu cuối cho tất cả các cột kiểu chuỗi
    string_columns = df.select_dtypes(include=['object']).columns

    for col in string_columns:
        df[col] = df[col].str.strip()
    df = df.dropna(subset=['tietkiem', 'giahientai', 'giathitruong', 'sotrang', 'trongluong'])

    # Chuyển đổi kiểu dữ liệu
    # Kiểm tra và xử lý dữ liệu trong cột 'tietkiem'
    df['tietkiem'] = df['tietkiem'].apply(
        lambda x: re.sub(r'[^\d]', '', str(x).split('(')[0].strip()) if pd.notnull(x) else '0'
    )

    # Chuyển thành kiểu số và thay thế các giá trị không hợp lệ thành NaN
    df['tietkiem'] = pd.to_numeric(df['tietkiem'], errors='coerce')

    # Loại bỏ các dòng có giá trị 'tietkiem' là NaN hoặc 0
    df = df[df['tietkiem'].notna() & (df['tietkiem'] != 0)]

    # Chuyển cột 'tietkiem' thành int sau khi xử lý
    df['tietkiem'] = df['tietkiem'].astype(int)
        # Xóa "Ngày xuất bản: " khỏi cột 'ngayxuatban' nếu tồn tại
    df['ngayxuatban'] = df['ngayxuatban'].str.replace('Ngày xuất bản: ', '', regex=False).str.strip()
    df['coursename'] = df['coursename'].astype(str)
    df['tacgia'] = df['tacgia'].astype(str)
    df['giahientai'] = pd.to_numeric(df['giahientai'].replace('', None), errors='coerce').fillna(0).astype(int)
    df['giathitruong'] = pd.to_numeric(df['giathitruong'].replace('', None), errors='coerce').fillna(0).astype(int)
    df['tinhtrang'] = df['tinhtrang'].astype(str)
    df['dichgia'] = df['dichgia'].replace('', 'không có').fillna('không có').astype(str)
    df['nhaxuatban'] = df['nhaxuatban'].astype(str)
    df['sotrang'] = pd.to_numeric(df['sotrang'].replace('', None), errors='coerce').fillna(0).astype(int)
    df['trongluong'] = pd.to_numeric(df['trongluong'].replace('', None), errors='coerce').fillna(0).astype(int)
    df['noidung_brand'] = df['noidung_brand'].astype(str)
    # Xuất DataFrame ra tệp Excel
    df = df[df['ngayxuatban'] != 'Đang cập nhật']
    print(f"Dữ liệu đã được chuyển thành DataFrame: {df.head()}")  # In ra 5 dòng đầu của DataFrame để kiểm tra

    df.to_excel('output.xlsx', index=False)
    print("Dữ liệu đã được xuất ra file Excel.")

# Kết nối tới PostgreSQL
conn = psycopg2.connect(
    dbname='dbmyscrapybook',
    user='postgres',
    password='123',
    host='postgres_container',
    port='5432',
    options='-c client_encoding=UTF8'
)

cursor = conn.cursor()

# Tạo bảng nếu chưa có
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

# Commit để tạo bảng
conn.commit()

# Biến đếm số bản ghi được chèn thành công
insert_count = 0

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

    insert_count += 1  # Tăng đếm sau khi chèn thành công

# Commit để chèn dữ liệu
conn.commit()

print(f"Số dữ liệu đẩy vào PostgreSQL thành công: {insert_count}")  # In ra số dữ liệu chèn thành công

# Đóng kết nối
cursor.close()
conn.close()
