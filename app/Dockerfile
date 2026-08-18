# Python 应用 Dockerfile
# 作用：构建 consumer 和 celery-worker 共用的镜像
# consumer 启动命令：python consumer.py
# celery-worker 启动命令：celery -A celery_app worker --loglevel=info

FROM python:3.9-slim

WORKDIR /app

# 使用阿里云PyPI源加速安装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/

# 复制应用代码
COPY . .

# 默认启动consumer，celery-worker在docker-compose中通过command覆盖
CMD ["python", "consumer.py"]
