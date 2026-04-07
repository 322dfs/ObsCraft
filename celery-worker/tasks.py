import os
from celery import Celery
import requests
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pymysql
from datetime import datetime

broker_url = os.getenv('CELERY_BROKER_URL', 'redis://redis:6379/0')
result_backend = os.getenv('CELERY_RESULT_BACKEND', 'redis://redis:6379/0')

celery = Celery(
    'alert_tasks',
    broker=broker_url,
    backend=result_backend
)

celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Shanghai',
    enable_utc=True,
    task_routes={
        'tasks.send_email_alert': {'queue': 'email'},
        'tasks.send_dingtalk_alert': {'queue': 'dingtalk'},
    }
)

SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.qq.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
SMTP_USER = os.getenv('SMTP_USER', '')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', '')

DINGTALK_WEBHOOK = os.getenv('DINGTALK_WEBHOOK', '')

MYSQL_HOST = os.getenv('MYSQL_HOST', 'subencai-mysql')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'root123')
MYSQL_DB = os.getenv('MYSQL_DB', 'log_monitor')

def get_db_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

@celery.task(bind=True, max_retries=3, default_retry_delay=60)
def send_email_alert(self, alert_data):
    if not SMTP_USER or not SMTP_PASSWORD:
        print("邮件告警未配置SMTP信息，跳过发送")
        return {"status": "skipped", "reason": "SMTP not configured"}
    
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"[告警] {alert_data.get('alert_type', 'unknown').upper()} - {alert_data.get('status_code', 'N/A')}"
        msg['From'] = SMTP_USER
        msg['To'] = SMTP_USER
        
        html_content = f"""
        <html>
        <body>
        <h2>日志告警通知</h2>
        <table border="1" cellpadding="10">
            <tr><td><b>告警类型</b></td><td>{alert_data.get('alert_type', 'N/A')}</td></tr>
            <tr><td><b>状态码</b></td><td>{alert_data.get('status_code', 'N/A')}</td></tr>
            <tr><td><b>IP地址</b></td><td>{alert_data.get('ip_address', 'N/A')}</td></tr>
            <tr><td><b>请求URL</b></td><td>{alert_data.get('url', 'N/A')}</td></tr>
            <tr><td><b>告警时间</b></td><td>{alert_data.get('created_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}</td></tr>
            <tr><td><b>错误信息</b></td><td>{alert_data.get('message', 'N/A')}</td></tr>
        </table>
        <h3>AI分析结果</h3>
        <pre>{alert_data.get('ai_analysis', '暂无分析')}</pre>
        <h3>修复建议</h3>
        <pre>{alert_data.get('fix_suggestion', '暂无建议')}</pre>
        </body>
        </html>
        """
        
        msg.attach(MIMEText(html_content, 'html', 'utf-8'))
        
        import ssl
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        
        server = smtplib.SMTP_SSL(SMTP_SERVER, 465, context=context)
        server.set_debuglevel(1)
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, SMTP_USER, msg.as_string())
        server.quit()
        
        print(f"邮件告警发送成功: {alert_data.get('id')}")
        return {"status": "success", "alert_id": alert_data.get('id')}
        
    except Exception as e:
        print(f"邮件发送失败: {str(e)}")
        raise self.retry(exc=e)

@celery.task(bind=True, max_retries=3, default_retry_delay=60)
def send_dingtalk_alert(self, alert_data):
    if not DINGTALK_WEBHOOK:
        print("钉钉告警未配置Webhook，跳过发送")
        return {"status": "skipped", "reason": "DingTalk webhook not configured"}
    
    try:
        alert_type = alert_data.get('alert_type', 'unknown')
        status_code = alert_data.get('status_code', 'N/A')
        url = alert_data.get('url', 'N/A')
        message = alert_data.get('message', 'N/A')
        ai_analysis = alert_data.get('ai_analysis', '暂无分析')
        fix_suggestion = alert_data.get('fix_suggestion', '暂无建议')
        
        markdown_content = f"""## 日志告警通知

**告警类型**: {alert_type}

**状态码**: {status_code}

**请求URL**: {url}

**错误信息**: {message}

---

### AI分析结果
{ai_analysis}

### 修复建议
{fix_suggestion}

---
*时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": f"日志告警 - {alert_type}",
                "text": markdown_content
            }
        }
        
        headers = {'Content-Type': 'application/json'}
        response = requests.post(DINGTALK_WEBHOOK, json=payload, headers=headers, timeout=10)
        
        if response.status_code == 200:
            result = response.json()
            if result.get('errcode') == 0:
                print(f"钉钉告警发送成功: {alert_data.get('id')}")
                return {"status": "success", "alert_id": alert_data.get('id')}
            else:
                raise Exception(f"钉钉API错误: {result.get('errmsg')}")
        else:
            raise Exception(f"HTTP错误: {response.status_code}")
            
    except Exception as e:
        print(f"钉钉发送失败: {str(e)}")
        raise self.retry(exc=e)

@celery.task
def send_all_alerts(alert_data):
    results = {
        'email': None,
        'dingtalk': None
    }
    
    try:
        email_result = send_email_alert.delay(alert_data)
        results['email'] = email_result.id
    except Exception as e:
        results['email_error'] = str(e)
    
    try:
        dingtalk_result = send_dingtalk_alert.delay(alert_data)
        results['dingtalk'] = dingtalk_result.id
    except Exception as e:
        results['dingtalk_error'] = str(e)
    
    return results

@celery.task
def check_high_traffic():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) as count 
            FROM logs 
            WHERE timestamp >= DATE_SUB(NOW(), INTERVAL 5 MINUTE)
        """)
        result = cursor.fetchone()
        request_count = result['count']
        
        if request_count > 100:
            alert_data = {
                'alert_type': 'high_traffic',
                'status_code': 'N/A',
                'ip_address': 'N/A',
                'url': 'N/A',
                'message': f'5分钟内请求数: {request_count}，超过阈值100',
                'ai_analysis': '流量异常，可能存在攻击或业务高峰',
                'fix_suggestion': '建议检查服务负载，必要时进行限流或扩容'
            }
            send_all_alerts.delay(alert_data)
            
            cursor.execute("""
                INSERT INTO alerts (alert_type, message, ai_analysis, fix_suggestion)
                VALUES (%s, %s, %s, %s)
            """, ('high_traffic', alert_data['message'], alert_data['ai_analysis'], alert_data['fix_suggestion']))
            conn.commit()
        
        cursor.close()
        conn.close()
        
        return {"status": "checked", "request_count": request_count}
        
    except Exception as e:
        print(f"流量检查失败: {str(e)}")
        return {"status": "error", "message": str(e)}

@celery.task
def cleanup_old_logs():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            DELETE FROM logs 
            WHERE timestamp < DATE_SUB(NOW(), INTERVAL 7 DAY)
        """)
        deleted_count = cursor.rowcount
        conn.commit()
        
        cursor.close()
        conn.close()
        
        return {"status": "success", "deleted_count": deleted_count}
        
    except Exception as e:
        print(f"日志清理失败: {str(e)}")
        return {"status": "error", "message": str(e)}
