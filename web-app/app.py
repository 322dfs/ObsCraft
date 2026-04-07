from flask import Flask, jsonify, request
import logging
import os
import time
from datetime import datetime
import random
import mysql.connector
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from functools import wraps
import requests
import redis
import json

app = Flask(__name__)

REQUEST_COUNT = Counter('web_app_requests_total', 'Total request count', ['method', 'endpoint', 'status'])
REQUEST_LATENCY = Histogram('web_app_request_duration_seconds', 'Request latency', ['endpoint'])
ACTIVE_REQUESTS = Gauge('web_app_active_requests', 'Active requests')
DB_CONNECTIONS = Gauge('web_app_db_connections', 'Database connections')

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY', '')
DEEPSEEK_API_URL = os.getenv('DEEPSEEK_API_URL', 'https://api.deepseek.com/v1/chat/completions')
SMTP_USER = os.getenv('SMTP_USER', '2080981057@qq.com')

def get_redis():
    return redis.Redis(host=REDIS_HOST, port=6379, db=0)

def call_deepseek_api(error_info):
    if not DEEPSEEK_API_KEY:
        return 'AI分析未配置'
    
    prompt = f"""作为运维专家，请分析以下错误日志并提供修复建议：

错误信息：
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
            return result['choices'][0]['message']['content']
    except Exception as e:
        print(f"DeepSeek API调用失败: {e}")
    
    return 'AI分析暂时不可用'

def send_email_alert_direct(alert_data):
    try:
        r = get_redis()
        task_data = json.dumps({
            'task': 'tasks.send_email_alert',
            'args': [alert_data],
            'kwargs': {}
        })
        r.lpush('celery', json.dumps({
            'body': task_data,
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'properties': {
                'correlation_id': str(time.time()),
                'reply_to': '',
                'delivery_mode': 2
            }
        }))
        print(f"邮件告警任务已提交: {alert_data.get('id')}")
        return True
    except Exception as e:
        print(f"提交邮件告警任务失败: {e}")
        return False

def track_requests(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        ACTIVE_REQUESTS.inc()
        start_time = time.time()
        try:
            result = f(*args, **kwargs)
            status_code = result[1] if isinstance(result, tuple) else 200
            REQUEST_COUNT.labels(method=request.method, endpoint=request.endpoint, status=status_code).inc()
            return result
        finally:
            REQUEST_LATENCY.labels(endpoint=request.endpoint).observe(time.time() - start_time)
            ACTIVE_REQUESTS.dec()
    return decorated_function

log_dir = '/app/logs'
os.makedirs(log_dir, exist_ok=True)

file_handler = logging.FileHandler(f'{log_dir}/app.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
app.logger.addHandler(file_handler)

MYSQL_HOST = os.getenv('MYSQL_HOST', 'subencai-mysql')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'root123')
MYSQL_DB = os.getenv('MYSQL_DB', 'log_monitor')

def get_db():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )

@app.route('/health')
@track_requests
def health():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

@app.route('/')
@track_requests
def index():
    app.logger.info(f"访问首页 - IP: {request.remote_addr}")
    return jsonify({
        'message': 'Web服务运行正常',
        'service': 'log-monitor-demo',
        'time': datetime.now().isoformat()
    })

@app.route('/metrics')
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

@app.route('/api/data')
def get_data():
    app.logger.info(f"获取数据 - IP: {request.remote_addr}")
    return jsonify({
        'data': [
            {'id': 1, 'name': '服务A', 'status': 'running'},
            {'id': 2, 'name': '服务B', 'status': 'running'},
            {'id': 3, 'name': '服务C', 'status': 'stopped'}
        ]
    })

@app.route('/api/error/404')
def trigger_404():
    ip = request.remote_addr
    app.logger.warning(f"404错误触发 - IP: {ip} - 路径不存在")
    
    try:
        ai_analysis = call_deepseek_api({
            'status': 404,
            'ip': ip,
            'url': '/api/error/404',
            'message': '页面未找到'
        })
        
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO alerts (alert_type, status_code, url, message, ai_analysis) VALUES (%s, %s, %s, %s, %s)",
            ('warning', 404, '/api/error/404', f'404错误触发 - IP: {ip} - 路径不存在', ai_analysis)
        )
        conn.commit()
        alert_id = cursor.lastrowid
        cursor.close()
        conn.close()
        
        alert_data = {
            'id': alert_id,
            'alert_type': 'warning',
            'status_code': 404,
            'ip_address': ip,
            'url': '/api/error/404',
            'message': f'404错误触发 - IP: {ip} - 路径不存在',
            'ai_analysis': ai_analysis,
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        r = get_redis()
        r.lpush('celery', json.dumps({
            'body': json.dumps({
                'task': 'tasks.send_email_alert',
                'id': str(time.time()),
                'args': [alert_data],
                'kwargs': {}
            }),
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'properties': {
                'correlation_id': str(time.time()),
                'reply_to': '',
                'delivery_tag': str(time.time())
            }
        }))
        
    except Exception as e:
        print(f"创建告警失败: {e}")
    
    return jsonify({'error': 'Not Found', 'code': 404}), 404

@app.route('/api/error/500')
def trigger_500():
    ip = request.remote_addr
    app.logger.error(f"500错误触发 - IP: {ip} - 服务器内部错误")
    
    try:
        ai_analysis = call_deepseek_api({
            'status': 500,
            'ip': ip,
            'url': '/api/error/500',
            'message': '服务器内部错误'
        })
        
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO alerts (alert_type, status_code, url, message, ai_analysis) VALUES (%s, %s, %s, %s, %s)",
            ('error', 500, '/api/error/500', f'500错误触发 - IP: {ip} - 服务器内部错误', ai_analysis)
        )
        conn.commit()
        alert_id = cursor.lastrowid
        cursor.close()
        conn.close()
        
        alert_data = {
            'id': alert_id,
            'alert_type': 'error',
            'status_code': 500,
            'ip_address': ip,
            'url': '/api/error/500',
            'message': f'500错误触发 - IP: {ip} - 服务器内部错误',
            'ai_analysis': ai_analysis,
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        r = get_redis()
        r.lpush('celery', json.dumps({
            'body': json.dumps({
                'task': 'tasks.send_email_alert',
                'id': str(time.time()),
                'args': [alert_data],
                'kwargs': {}
            }),
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'properties': {
                'correlation_id': str(time.time()),
                'reply_to': '',
                'delivery_tag': str(time.time())
            }
        }))
        
    except Exception as e:
        print(f"创建告警失败: {e}")
    
    return jsonify({'error': 'Internal Server Error', 'code': 500}), 500

@app.route('/api/services')
def list_services():
    services = [
        {'name': 'web-app', 'status': 'running', 'port': 5000},
        {'name': 'nginx', 'status': 'running', 'port': 80},
        {'name': 'mysql', 'status': 'running', 'port': 3306},
        {'name': 'redis', 'status': 'running', 'port': 6379},
        {'name': 'kafka', 'status': 'running', 'port': 9092},
        {'name': 'prometheus', 'status': 'running', 'port': 9090},
        {'name': 'grafana', 'status': 'running', 'port': 3000}
    ]
    app.logger.info(f"查询服务列表 - IP: {request.remote_addr}")
    return jsonify({'services': services})

@app.route('/api/services', methods=['POST'])
def add_service():
    data = request.json
    app.logger.info(f"添加新服务 - IP: {request.remote_addr} - 服务名: {data.get('name')}")
    return jsonify({
        'message': '服务添加成功',
        'service': data
    }), 201

@app.route('/api/metrics')
def api_metrics():
    return jsonify({
        'cpu_usage': random.uniform(10, 50),
        'memory_usage': random.uniform(20, 60),
        'disk_usage': random.uniform(30, 70),
        'requests_per_second': random.randint(10, 100),
        'active_connections': random.randint(5, 50)
    })

@app.route('/api/logs')
def get_logs():
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM logs ORDER BY timestamp DESC LIMIT 50")
        logs = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify({'logs': logs})
    except Exception as e:
        return jsonify({'logs': [], 'error': str(e)})

@app.route('/api/alerts')
def get_alerts():
    try:
        conn = get_db()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM alerts ORDER BY timestamp DESC LIMIT 20")
        alerts = cursor.fetchall()
        cursor.close()
        conn.close()
        return jsonify({'alerts': alerts})
    except Exception as e:
        return jsonify({'alerts': [], 'error': str(e)})

@app.route('/api/alerts/<int:alert_id>/read', methods=['POST'])
def mark_alert_read(alert_id):
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("UPDATE alerts SET is_read = TRUE WHERE id = %s", (alert_id,))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'message': '已标记为已读'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/test-alert', methods=['POST'])
def test_alert():
    try:
        data = request.get_json(silent=True) or {}
        error_type = data.get('type', '500')
        url = data.get('url', '/api/test')
        
        ai_analysis_404 = """错误原因：请求的资源不存在
影响范围：用户无法访问该页面
修复步骤：
1. 检查URL路径是否正确
2. 确认资源是否已被删除或移动
3. 检查路由配置是否正确
4. 考虑添加重定向或自定义404页面"""
        
        ai_analysis_500 = """错误原因：服务器内部错误
影响范围：用户请求无法正常处理
修复步骤：
1. 查看服务器错误日志定位具体问题
2. 检查数据库连接是否正常
3. 检查代码逻辑是否存在异常
4. 重启相关服务
5. 如问题持续，联系开发人员处理"""
        
        ai_analysis = ai_analysis_404 if error_type == '404' else ai_analysis_500
        status_code = 404 if error_type == '404' else 500
        alert_type = 'warning' if error_type == '404' else 'error'
        message = '页面未找到' if error_type == '404' else '服务器内部错误'
        
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO alerts (alert_type, status_code, url, message, ai_analysis) VALUES (%s, %s, %s, %s, %s)",
            (alert_type, status_code, url, message, ai_analysis)
        )
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({
            'message': '测试告警已创建',
            'alert': {
                'type': alert_type,
                'status_code': status_code,
                'url': url,
                'ai_analysis': ai_analysis
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
