import trino
from trino.dbapi import connect

# Kết nối tới Trino
conn = connect(
    host='localhost',  # hoặc địa chỉ IP của máy chủ Trino
    port=8080,         # cổng mặc định của Trino
    user='trino',  # thay bằng tên người dùng
    catalog='saleholding2',
    schema='public',
    http_scheme='http'  # nếu sử dụng https, thay bằng 'https'
)

# Hàm để thực thi truy vấn
def execute_query(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()

# Truy vấn để lấy danh sách các bảng trong saleholding2.public
tables1_query = "SHOW TABLES IN saleholding2.public"
tables1 = execute_query(conn, tables1_query)
print(len(tables1))

tables2_query = "SHOW TABLES IN saleholding.public"
tables2 = execute_query(conn, tables2_query)
print(len(tables2))

tables1 = [tbl for tbl in tables1 if tbl not in tables2]
print(len(tables1))

# Lặp qua danh sách các bảng và tạo bảng mới trong saleholding.public
for table in tables1:
    table_name = table[0]
    if 'log' in table_name or 'mail' in table_name:
        continue

    create_table_query = f"""
        CREATE TABLE saleholding.public.{table_name} AS 
        SELECT * FROM saleholding2.public.{table_name}
    """
    print(f"Creating table: saleholding.public.{table_name}")
    try:
        execute_query(conn, create_table_query)
        print(f"Table {table_name} created successfully.")
    except Exception as e:
        print(f"Failed to create table {table_name}: {e}")

print("Tables created successfully.")
