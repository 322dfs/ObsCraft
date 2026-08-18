-- MySQL 初始化脚本
-- 容器启动时自动执行，创建数据库和表

CREATE DATABASE IF NOT EXISTS log_platform;
USE log_platform;

CREATE TABLE IF NOT EXISTS access_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    server_name VARCHAR(20) COMMENT '来源服务器(web-1/web-2/web-3)',
    remote_addr VARCHAR(50) COMMENT '客户端IP',
    remote_user VARCHAR(50) COMMENT '用户名',
    time_local DATETIME COMMENT '请求时间',
    request_method VARCHAR(10) COMMENT '请求方法(GET/POST等)',
    request_url TEXT COMMENT '请求URL',
    status_code INT COMMENT 'HTTP状态码(200/404/500等)',
    body_bytes_sent BIGINT COMMENT '响应体大小(字节)',
    http_referer TEXT COMMENT '来源页面',
    http_user_agent TEXT COMMENT '客户端User-Agent',
    request_time FLOAT COMMENT '响应时间(秒)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
    INDEX idx_status (status_code),
    INDEX idx_time (time_local),
    INDEX idx_server (server_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Nginx访问日志表';

-- 创建Prometheus mysqld-exporter监控专用用户
CREATE USER IF NOT EXISTS 'monitor'@'%' IDENTIFIED BY 'Monitor123';
GRANT PROCESS, REPLICATION CLIENT, SELECT ON *.* TO 'monitor'@'%';
FLUSH PRIVILEGES;
