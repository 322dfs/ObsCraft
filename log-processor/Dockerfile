FROM demo-backend:latest

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

COPY processor.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "processor.py"]
