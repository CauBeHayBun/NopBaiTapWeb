# # For more information, please refer to https://aka.ms/vscode-docker-python
# FROM python:3-slim

# # Keeps Python from generating .pyc files in the container
# ENV PYTHONDONTWRITEBYTECODE=1

# # Turns off buffering for easier container logging
# ENV PYTHONUNBUFFERED=1

# # Install pip requirements
# COPY requirements.txt .
# RUN python -m pip install -r requirements.txt

# WORKDIR /app
# COPY . /app

# # Creates a non-root user with an explicit UID and adds permission to access the /app folder
# # For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
# RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
# USER appuser

# # During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
# CMD ["python", "xulydulieu_postgres\app.py"]


# Sử dụng Python làm hình ảnh gốc
FROM python:3.9

# Đặt thư mục làm việc
WORKDIR /app

# Sao chép các tệp yêu cầu
COPY requirements.txt .

# Cài đặt các thư viện yêu cầu
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép mã nguồn
COPY . /app

# Lệnh để chạy ứng dụng
CMD ["sh", "-c", "python xulydulieu_postgres/app.py"]
