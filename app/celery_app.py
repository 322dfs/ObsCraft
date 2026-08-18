"""
Celery 应用定义
作用：创建Celery实例，配置Redis作为broker和backend
关系：tasks.py 从这里导入 app 定义任务，consumer.py 从 tasks 导入 send_alert
"""
import os
from celery import Celery

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')

app = Celery('alert_tasks')
app.conf.update(
    broker_url=f'redis://{REDIS_HOST}:{REDIS_PORT}/0',
    result_backend=f'redis://{REDIS_HOST}:{REDIS_PORT}/1',
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Shanghai',
    enable_utc=True,
)

# 在末尾导入任务模块，确保任务被注册到Celery
import tasks
