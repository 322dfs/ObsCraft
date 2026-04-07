FROM demo-backend:latest

WORKDIR /app

COPY app.py .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

RUN mkdir -p /app/logs

EXPOSE 5000

CMD ["python", "app.py"]
