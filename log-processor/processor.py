import json
import re
import os
import time
import requests
from datetime import datetime
from kafka import KafkaConsumer
import mysql.connector
import redis
import sys
sys.path.append('/app/celery_tasks')
from tasks import send_all_alerts

KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'kafka:9092')
MYSQL_HOST = os.getenv('MYSQL_HOST', 'subencai-mysql')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'root123')
MYSQL_DB = os.getenv('MYSQL_DB', 'log_monitor')
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY', 'sk-b1883c383abc4e368d75229268bcacbb')
DEEPSEEK_API_URL = os.getenv('DEEPSEEK_API_URL', 'https://api.deepseek.com/v1/chat/completions')

def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )

def get_redis():
    return redis.Redis(host=REDIS_HOST, port=6379, db=0)

def parse_nginx_log(log_line):
    pattern = r'(\d+\.\d+\.\d+\.\d+) - - \[([^\]]+)\] "(\w+) ([^"]+) HTTP/[^"]+" (\d+) (\d+)'
    match = re.match(pattern, log_line)
    if match:
        return {
            'ip': match.group(1),
            'timestamp': match.group(2),
            'method': match.group(3),
            'url': match.group(4),
            'status': int(match.group(5)),
            'size': int(match.group(6))
        }
    return None

def parse_webapp_log(log_line):
    pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+) - (\w+) - (.+)'
    match = re.match(pattern, log_line)
    if match:
        return {
            'timestamp': match.group(1),
            'level': match.group(2),
            'message': match.group(3)
        }
    return None

def save_log(log_data):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO logs (source, log_type, ip, method, url, status, message, raw)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            log_data.get('source', 'unknown'),
            log_data.get('log_type', 'info'),
            log_data.get('ip', ''),
            log_data.get('method', ''),
            log_data.get('url', ''),
            log_data.get('status', 0),
            log_data.get('message', ''),
            log_data.get('raw', '')
        ))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

def call_deepseek_api(error_info):
    if not DEEPSEEK_API_KEY:
        return {
            'analysis': 'AI分析未配置',
            'suggestion': '请配置DEEPSEEK_API_KEY环境变量'
        }
    
    prompt = f"""作为运维专家，请分析以下错误日志并提供修复建议：

错误信息：
- 来源：{error_info.get('source', 'unknown')}
- 状态码：{error_info.get('status', 'unknown')}
- IP地址：{error_info.get('ip', 'unknown')}
- URL：{error_info.get('url', 'unknown')}
- 消息：{error_info.get('message', '')}

请用简洁的中文回答：
1. 错误原因（一句话）
2. 可能影响（一句话）
3. 修复步骤（3条以内）
"""
    
    try:
        response = requests.post(
            DEEPSEEK_API_URL,
            headers={
                'Authorization': f'Bearer {DEEPSEEK_API_KEY}',
                'Content-Type': 'application/json'
            },
            json={
                'model': 'deepseek-chat',
                'messages': [{'role': 'user', 'content': prompt}],
                'max_tokens': 500
            },
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result['choices'][0]['message']['content']
            return {
                'analysis': content,
                'suggestion': content
            }
    except Exception as e:
        print(f"DeepSeek API调用失败: {e}")
    
    return {
        'analysis': 'AI分析暂时不可用',
        'suggestion': '请检查API配置或稍后重试'
    }

def create_alert(log_data):
    ai_result = call_deepseek_api(log_data)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO alerts (alert_type, status_code, url, message, ai_analysis)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            'error' if log_data.get('status', 0) >= 500 else 'warning',
            log_data.get('status', 0),
            log_data.get('url', ''),
            log_data.get('message', ''),
            ai_result['analysis']
        ))
        conn.commit()
        
        alert_id = cursor.lastrowid
        
        alert_data = {
            'id': alert_id,
            'alert_type': 'error' if log_data.get('status', 0) >= 500 else 'warning',
            'status_code': log_data.get('status', 0),
            'ip': log_data.get('ip', ''),
            'url': log_data.get('url', ''),
            'message': log_data.get('message', ''),
            'ai_analysis': ai_result['analysis'],
            'suggestion': ai_result['suggestion'],
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        try:
            send_all_alerts.delay(alert_data)
            print(f"告警任务已提交: ID={alert_id}")
        except Exception as e:
            print(f"提交告警任务失败: {e}")
        
        r = get_redis()
        r.publish('alerts', json.dumps({
            'type': 'new_alert',
            'id': alert_id,
            'status_code': log_data.get('status'),
            'url': log_data.get('url'),
            'ai_analysis': ai_result['analysis']
        }))
    finally:
        cursor.close()
        conn.close()

def process_log(message):
    try:
        data = json.loads(message.value)
        raw_log = data.get('message', '')
        source = data.get('source', 'unknown')
        log_type = data.get('type', 'info')
        
        if source == 'nginx':
            parsed = parse_nginx_log(raw_log)
            if parsed:
                log_data = {
                    'source': source,
                    'log_type': log_type,
                    'ip': parsed['ip'],
                    'method': parsed['method'],
                    'url': parsed['url'],
                    'status': parsed['status'],
                    'message': raw_log,
                    'raw': raw_log
                }
                save_log(log_data)
                
                if parsed['status'] >= 400:
                    create_alert(log_data)
        
        elif source == 'webapp':
            parsed = parse_webapp_log(raw_log)
            if parsed:
                log_data = {
                    'source': source,
                    'log_type': parsed['level'],
                    'message': parsed['message'],
                    'raw': raw_log
                }
                save_log(log_data)
                
                if parsed['level'] in ['ERROR', 'WARNING']:
                    create_alert(log_data)
    
    except Exception as e:
        print(f"处理日志失败: {e}")

def main():
    print("等待Kafka就绪...")
    time.sleep(30)
    
    consumer = KafkaConsumer(
        'logs',
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='log-processor',
        value_deserializer=lambda x: x.decode('utf-8', errors='ignore')
    )
    
    print("日志处理器已启动，等待消息...")
    
    for message in consumer:
        process_log(message)

if __name__ == '__main__':
    main()
