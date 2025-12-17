#[root@kafka1 log_consumer]# cat consumer.py
#CI/CD TEST v2 - auto deployed at 2025-11-23 via git push
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import re
import time
import requests
from datetime import datetime
import pymysql
from kafka import KafkaConsumer
import redis
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import threading
import os  # ← 新增：用于读取环境变量


# Redis连接
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# MySQL配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Sctl@123456789',
    'database': 'log_analysis',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor
}

# 邮箱配置
EMAIL_CONFIG = {
    'smtp_server': 'smtp.qq.com',
    'smtp_port': 465,                     # ← 必须是 465
    'email': '2080981057@qq.com',
    'password': 'bvntpipdhwudbeag',       # 授权码（正确）
    'to_email': '2080981057@qq.com'
}
# 高流量阈值
HIGH_TRAFFIC_THRESHOLD = 5
HIGH_TRAFFIC_WINDOW = 60

def get_ip_location(ip):
    """获取IP地理位置信息"""
    try:
        if ip in ['127.0.0.1', 'localhost', 'kafka1']:
            return "本地服务器"
        response = requests.get("http://ip-api.com/json/{}?lang=zh-CN".format(ip), timeout=3)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'success':
                return f"{data.get('country', '')}-{data.get('city', '')}"
        return "未知位置"
    except Exception as e:
        print(f"[WARN] IP定位失败 {ip}: {e}")
        return "定位失败"

def parse_nginx_log(log_message):
    """解析Nginx访问日志（适配你的简化格式：IP - - [时间] "请求" 状态码 字节数）"""
    pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d+) (\d+)'
    match = re.match(pattern, log_message)
    if match:
        return {
            'client_ip': match.group(1),
            'timestamp_str': match.group(2),
            'request': match.group(3),
            'status_code': int(match.group(4)),
            'response_size': int(match.group(5)),
            'referrer': '',
            'user_agent': ''
        }
    return None



def send_email_async(subject, body):
    """异步发送邮件（使用 SMTP_SSL + 465 端口）"""
    def send():
        try:
            msg = MIMEText(body, 'plain', 'utf-8')
            msg['From'] = EMAIL_CONFIG['email']
            msg['To'] = EMAIL_CONFIG['to_email']
            msg['Subject'] = Header(subject, 'utf-8')
            
            # 使用 SMTP_SSL 直接建立加密连接（端口 465）
            server = smtplib.SMTP_SSL(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
            server.login(EMAIL_CONFIG['email'], EMAIL_CONFIG['password'])
            server.sendmail(EMAIL_CONFIG['email'], [EMAIL_CONFIG['to_email']], msg.as_string())
            server.quit()
            print(f"[ALERT] 邮件发送成功: {subject}")
        except Exception as e:
            print(f"[ERROR] 发送邮件失败: {e}")
    thread = threading.Thread(target=send)
    thread.daemon = True
    thread.start()


def send_dingtalk_async(msg):
    """异步发送钉钉告警（从环境变量 DINGTALK_TOKEN 读取）"""
    token = os.getenv("DINGTALK_TOKEN")
    if not token:
        print("[WARN] DINGTALK_TOKEN 未设置，跳过钉钉告警")
        return

    def send():
        try:
            webhook = f"https://oapi.dingtalk.com/robot/send?access_token={token}"
            payload = {
                "msgtype": "text",
                "text": {
                    "content": msg.strip()
                },
                "at": {
                    "isAtAll": False
                }
            }
            resp = requests.post(webhook, json=payload, timeout=5)
            if resp.status_code == 200 and resp.json().get("errcode") == 0:
                print("[DINGTALK] 告警发送成功")
            else:
                print(f"[DINGTALK] 发送失败: {resp.text}")
        except Exception as e:
            print(f"[DINGTALK] 异常: {e}")

    thread = threading.Thread(target=send)
    thread.daemon = True
    thread.start()


def is_high_traffic(client_ip):
    """检查是否高流量"""
    try:
        current_minute = datetime.now().strftime('%Y-%m-%d %H:%M')
        key = f"traffic:{client_ip}:{current_minute}"
        current_count = redis_client.incr(key)
        redis_client.expire(key, HIGH_TRAFFIC_WINDOW + 10)
        return current_count >= HIGH_TRAFFIC_THRESHOLD
    except Exception as e:
        print(f"[ERROR] 流量检查失败: {e}")
        return False

def save_to_mysql(log_record):
    """存储日志到MySQL"""
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        query = """
        INSERT INTO nginx_access_logs 
        (log_type, server_name, client_ip, ip_location, request_method, request_url, 
         status_code, response_size, user_agent, referrer, timestamp, is_high_traffic, is_error)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            log_record['log_type'], 
            log_record['server_name'], 
            log_record['client_ip'],
            log_record['ip_location'],
            log_record['request_method'], 
            log_record['request_url'], 
            log_record['status_code'],
            log_record['response_size'], 
            log_record['user_agent'],
            log_record.get('referrer', ''),
            log_record['timestamp'], 
            1 if log_record['is_high_traffic'] else 0,
            1 if log_record['is_error'] else 0
        ))
        conn.commit()
        cursor.close()
        conn.close()
        print(f"[SUCCESS] 日志已存储: {log_record['client_ip']} -> {log_record['request_url']}")
    except Exception as e:
        print(f"[ERROR] 存储失败: {e}")

def process_log_message(message):
    """处理单条日志消息"""
    try:
        log_data = json.loads(message.value.decode('utf-8'))
        fields = log_data.get('fields', {})
        log_type = fields.get('log_type', '')
        server_name = fields.get('server_name', 'kafka1')
        raw_message = log_data.get('message', '')
        print(f"[INFO] 处理 {log_type} 日志")
        
        if log_type == "nginx-access":
            parsed = parse_nginx_log(raw_message)
            if not parsed:
                print(f"[WARN] 无法解析日志: {raw_message[:100]}...")
                return
            
            ip_location = get_ip_location(parsed['client_ip'])
            high_traffic = is_high_traffic(parsed['client_ip'])
            is_error = parsed['status_code'] >= 400
            
            request_parts = parsed['request'].split()
            request_method = request_parts[0] if request_parts else ''
            request_url = request_parts[1] if len(request_parts) > 1 else parsed['request']
            
            log_record = {
                'log_type': log_type,
                'server_name': server_name,
                'client_ip': parsed['client_ip'],
                'ip_location': ip_location,
                'request_method': request_method,
                'request_url': request_url,
                'status_code': parsed['status_code'],
                'response_size': parsed['response_size'],
                'user_agent': parsed['user_agent'],
                'referrer': parsed['referrer'],
                'timestamp': datetime.strptime(parsed['timestamp_str'], '%d/%b/%Y:%H:%M:%S %z'),
                'is_high_traffic': high_traffic,
                'is_error': is_error
            }
            
            save_to_mysql(log_record)
            
            if high_traffic or is_error:
                alert_type = "高流量" if high_traffic else "错误"
                subject = f"ALERT 服务器告警 - {alert_type}"
                body = f"""
{alert_type}告警
时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
服务器: {server_name}
客户端IP: {parsed['client_ip']}
地理位置: {ip_location}
请求URL: {request_url}
状态码: {parsed['status_code']}
用户代理: {parsed['user_agent'][:100]}
                """
                send_email_async(subject, body)
                send_dingtalk_async(body)  # ← 新增：钉钉告警
                print(f"[ALERT] {alert_type}告警: {parsed['client_ip']}")
                
        elif log_type == "nginx-error":
            subject = "ALERT 服务器告警 - Nginx错误"
            body = f"""
Nginx错误告警
时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
服务器: {server_name}
错误信息: {raw_message[:500]}
            """
            send_email_async(subject, body)
            send_dingtalk_async(body)  # ← 新增：钉钉告警
            print(f"[ALERT] Nginx错误告警")
            
    except Exception as e:
        print(f"[ERROR] 处理日志失败: {e}")

def main():
    """主函数"""
    print("=" * 50)
    print("🚀 启动Kafka日志消费者")
    print("=" * 50)
    
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        conn.close()
        print("OK MySQL连接成功")
        
        redis_client.ping()
        print("OK Redis连接成功")
        
        consumer = KafkaConsumer(
            'nginx-logs',
            bootstrap_servers = ["kafka1:9092", "kafka2:9092", "kafka3:9092"],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='log-consumer-group',
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        print("OK Kafka消费者创建成功")
        
        print("TARGET 开始监听日志...")
        print("按 Ctrl+C 停止消费者")
        
        for message in consumer:
            process_log_message(message)
            
    except KeyboardInterrupt:
        print("\n🛑 消费者停止")
    except Exception as e:
        print(f"ERROR 启动失败: {e}")

if __name__ == "__main__":
    main()
#[root@kafka1 log_consumer]# 