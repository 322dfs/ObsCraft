CREATE DATABASE IF NOT EXISTS log_monitor;
USE log_monitor;

CREATE TABLE IF NOT EXISTS logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50),
    log_type VARCHAR(50),
    ip_address VARCHAR(50),
    method VARCHAR(10),
    url VARCHAR(500),
    status_code INT,
    message TEXT,
    raw_log TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_timestamp (timestamp),
    INDEX idx_status (status_code),
    INDEX idx_source (source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS alerts (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    alert_type VARCHAR(50),
    status_code INT,
    ip_address VARCHAR(50),
    url VARCHAR(500),
    message TEXT,
    ai_analysis TEXT,
    fix_suggestion TEXT,
    is_read BOOLEAN DEFAULT FALSE,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_timestamp (timestamp),
    INDEX idx_type (alert_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS services (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    url VARCHAR(500),
    status VARCHAR(20) DEFAULT 'unknown',
    last_check DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO services (name, url, status) VALUES 
('web-app', 'http://web-app:5000', 'running'),
('nginx', 'http://nginx-gateway:80', 'running'),
('mysql', 'mysql:3306', 'running'),
('redis', 'redis:6379', 'running'),
('kafka', 'kafka:9092', 'running');
