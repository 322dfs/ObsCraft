import json
import re
import time
import requests
import ipaddress  # 新增：用于判断内网 IP
from datetime import datetime
import pymysql
from kafka import KafkaConsumer
import redis
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import threading
import os
import traceback

# ================== 全局配置 ==================
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

# 邮箱配置（可选）
EMAIL_CONFIG = {
    'smtp_server': 'smtp.qq.com',
    'smtp_port': 465,
    'email': '2080981057@qq.com',
    'password': 'qutpazgvgwujchfh',
    'to_email': '2080981057@qq.com'
}

# 告警冷却缓存
ALERT_COOLDOWN_CACHE = set()
ALERT_COOLDOWN_SECONDS = 60

# 高流量阈值
HIGH_TRAFFIC_THRESHOLD = 5
HIGH_TRAFFIC_WINDOW = 60

# ================== 工具函数 ==================

def should_send_alert(client_ip: str, alert_type: str) -> bool:
    key = f"{client_ip}:{alert_type}"
    if key in ALERT_COOLDOWN_CACHE:
        return False
    ALERT_COOLDOWN_CACHE.add(key)
    
    def cleanup():
        time.sleep(ALERT_COOLDOWN_SECONDS)
        ALERT_COOLDOWN_CACHE.discard(key)
    threading.Thread(target=cleanup, daemon=True).start()
    return True

def get_ip_location(ip):
    try:
        # 处理本地回环和主机名
        if ip in ['127.0.0.1', 'localhost', '::1']:
            return "本地服务器"
        
        # 判断是否为私有（内网）IP
        try:
            if ipaddress.ip_address(ip).is_private:
                return "内网地址"
        except ValueError:
            # 如果不是合法 IP（如 'kafka1'），跳过
            pass

        # 使用 ipinfo.io 查询
        token = os.getenv("IPINFO_TOKEN", "")
        url = f"https://ipinfo.io/{ip}/json"
        if token:
            url += f"?token={token}"
        
        response = requests.get(url, timeout=3)
        if response.status_code == 200:
            data = response.json()
            country = data.get('country', '').strip()
            city = data.get('city', '').strip()
            if country or city:
                return f"{country}-{city}"
        return "未知位置"
    except Exception as e:
        print(f"[WARN] IP定位失败 {ip}: {e}")
        return "未知位置"

def parse_nginx_log(log_message):
    pattern = r'(\S+) - - \[(.*?)\] "(.*?)" (\d+) (\d+) "(.*?)" "(.*?)"(?: "(.*?)")?'
    match = re.match(pattern, log_message)
    if match:
        return {
            'client_ip': match.group(1),
            'timestamp_str': match.group(2),
            'request': match.group(3),
            'status_code': int(match.group(4)),
            'response_size': int(match.group(5)),
            'referrer': match.group(6) or '',
            'user_agent': match.group(7) or ''
        }
    else:
        pattern_simple = r'(\S+) - - \[(.*?)\] "(.*?)" (\d+) (\d+)'
        match = re.match(pattern_simple, log_message)
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
    def send():
        try:
            msg = MIMEText(body, 'plain', 'utf-8')
            msg['From'] = EMAIL_CONFIG['email']
            msg['To'] = EMAIL_CONFIG['to_email']
            msg['Subject'] = Header(subject, 'utf-8')
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
    token = os.getenv("DINGTALK_TOKEN")
    if not token:
        print("[WARN] DINGTALK_TOKEN 未设置，跳过钉钉告警")
        return
    def send():
        try:
            webhook = f"https://oapi.dingtalk.com/robot/send?access_token={token}"
            payload = {
                "msgtype": "text",
                "text": {"content": msg.strip()},
                "at": {"isAtAll": False}
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
        traceback.print_exc()

# ================== 主处理逻辑 ==================

def process_log_message(message):
    try:
        log_data = json.loads(message.value.decode('utf-8'))
        raw_message = log_data.get('message', '')
        server_name = log_data.get('host', {}).get('name', 'unknown')

        tags = log_data.get('tags', [])
        fields = log_data.get('fields', {})
        raw_log_type_from_fields = fields.get('log_type', '')

        log_type = "unknown"
        if "nginx-access" in tags:
            log_type = "nginx-access"
        elif "nginx-error" in tags:
            log_type = "nginx-error"
        elif raw_log_type_from_fields == "nginx-access":
            log_type = "nginx-access"
        elif raw_log_type_from_fields == "nginx-error":
            log_type = "nginx-error"
        else:
            print(f"[DEBUG] 无法识别日志类型 → tags={tags}, fields.log_type='{raw_log_type_from_fields}'")

        print(f"[INFO] 处理 {log_type} 日志 (tags={tags}, fields.log_type='{raw_log_type_from_fields}')")

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
                if not should_send_alert(parsed['client_ip'], alert_type):
                    print(f"[SKIP] 告警冷却中: {parsed['client_ip']} ({alert_type})")
                    return
                
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
                """.strip()
                
                send_dingtalk_async(body)
                send_email_async(subject, body)
                print(f"[ALERT] {alert_type}告警: {parsed['client_ip']}")

        elif log_type == "nginx-error":
            if not should_send_alert(server_name, "nginx-error"):
                print(f"[SKIP] Nginx错误告警冷却中: {server_name}")
                return
                
            subject = "ALERT 服务器告警 - Nginx错误"
            body = f"""
Nginx错误告警
时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
服务器: {server_name}
错误信息: {raw_message[:500]}
            """.strip()
            
            send_dingtalk_async(body)
            print(f"[ALERT] Nginx错误告警")

    except Exception as e:
        print(f"[ERROR] 处理日志失败: {e}")
        traceback.print_exc()

# ================== 启动入口 ==================

def main():
    print("=" * 50)
    print("🚀 启动Kafka日志消费者（使用 ipinfo.io 定位）")
    print("=" * 50)
    
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        conn.close()
        print("OK MySQL连接成功")
        
        redis_client.ping()
        print("OK Redis连接成功")
        
        consumer = KafkaConsumer(
            'nginx-log',
            bootstrap_servers=["kafka1:9092", "kafka2:9092", "kafka3:9092"],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='log-consumer-group',
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        print("OK Kafka消费者创建成功")
        print("TARGET 开始监听日志...")
        print("💡 提示：设置 IPINFO_TOKEN 和 DINGTALK_TOKEN 环境变量以启用完整功能")
        print("按 Ctrl+C 停止消费者")
        
        for message in consumer:
            process_log_message(message)
            
    except KeyboardInterrupt:
        print("\n🛑 消费者停止")
    except Exception as e:
        print(f"ERROR 启动失败: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
