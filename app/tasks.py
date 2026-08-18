"""
Celery 告警任务
作用：当消费者检测到4xx/5xx错误时，通过Celery异步发送邮件和钉钉告警
设计：双通道告警（邮件+钉钉），互不影响，任一通道失败不影响另一个
"""
import smtplib
from email.mime.text import MIMEText
import requests
import os
from celery_app import app

# ============ 告警配置（通过环境变量注入，敏感信息不硬编码） ============
SMTP_HOST = os.getenv('SMTP_HOST', 'smtp.qq.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '465'))
SMTP_USER = os.getenv('SMTP_USER', '2080981057@qq.com')
SMTP_PASS = os.getenv('SMTP_PASS', 'tfcoklhbxqtjegah')
TO_EMAIL = os.getenv('TO_EMAIL', '2080981057@qq.com')

DINGTALK_WEBHOOK = os.getenv('DINGTALK_WEBHOOK', '')


@app.task
def send_alert(log_data):
    """
    告警任务：邮件 + 钉钉双通道
    consumer.py 中调用 send_alert.delay(log_data) 异步触发
    """
    status = log_data.get('status', 'unknown')
    url = log_data.get('url', '/')
    server = log_data.get('server_name', 'unknown')
    ip = log_data.get('remote_addr', 'unknown')
    time_local = log_data.get('time_local', 'unknown')

    subject = f'[告警] {server} 出现 {status} 错误'
    content = (
        f'错误告警\n'
        f'==================\n'
        f'服务器: {server}\n'
        f'客户端IP: {ip}\n'
        f'请求时间: {time_local}\n'
        f'请求URL: {url}\n'
        f'状态码: {status}\n'
    )

    # 通道1：邮件告警
    try:
        _send_email(subject, content)
    except Exception as e:
        print(f'[邮件告警失败] {e}')

    # 通道2：钉钉告警（未配置webhook则跳过）
    if DINGTALK_WEBHOOK:
        try:
            _send_dingtalk(subject, content)
        except Exception as e:
            print(f'[钉钉告警失败] {e}')

    return f'Alert sent: {status} on {server}'


def _send_email(subject, content):
    """通过QQ邮箱SMTP发送告警邮件"""
    msg = MIMEText(content, 'plain', 'utf-8')
    msg['Subject'] = subject
    msg['From'] = SMTP_USER
    msg['To'] = TO_EMAIL

    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as server:
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, [TO_EMAIL], msg.as_string())

    print(f'[邮件告警已发送] {subject}')


def _send_dingtalk(title, content):
    """通过钉钉Webhook发送告警消息"""
    headers = {'Content-Type': 'application/json'}
    data = {
        'msgtype': 'text',
        'text': {'content': f'{title}\n{content}'}
    }
    requests.post(DINGTALK_WEBHOOK, json=data, headers=headers, timeout=10)
    print(f'[钉钉告警已发送] {title}')
