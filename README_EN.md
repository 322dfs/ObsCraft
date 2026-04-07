# ObsCraft - Lightweight Log Monitoring Platform v3.0

## Overview

ObsCraft is a lightweight distributed log collection, real-time monitoring, and alerting platform designed for Kafka clusters.

### Core Features

- **Real-time Log Collection**: Collect Nginx access logs via Filebeat
- **Log Processing & Storage**: Kafka message queue + Python processor + MySQL storage
- **Intelligent Alerting**: AI-powered error analysis + Email/DingTalk notifications
- **Visual Monitoring**: Prometheus + Grafana monitoring dashboards
- **One-Click Deployment**: Docker Compose containerized deployment

---

## Version History

| Version | Key Features |
|---------|-------------|
| **v1.0** | Kafka Cluster (KRaft) + Filebeat + MySQL + Celery Alerting |
| **v2.0** | Added Prometheus monitoring + Grafana visualization |
| **v3.0** | Docker containerization + AI intelligent analysis + one-click deployment |

---

## Tech Stack

| Component | Version/Description |
|-----------|-------------------|
| Frontend | Vue.js 3 + HTML5 |
| Backend | Flask + Gunicorn |
| Message Queue | Kafka + Zookeeper |
| Cache | Redis |
| Database | MySQL 8.0 |
| Log Collection | Filebeat |
| Monitoring | Prometheus + Grafana |
| Task Queue | Celery + Redis |
| AI Analysis | DeepSeek API |
| Containerization | Docker + Docker Compose |

---

## System Architecture

```
                    ┌─────────────────┐
                    │   Browser       │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │   Nginx (8080)  │
                    └────────┬────────┘
                             │
            ┌────────────────┼────────────────┐
            │                │                │
    ┌───────▼───────┐ ┌──────▼──────┐ ┌──────▼──────┐
    │   Frontend    │ │   Web App   │ │  Grafana    │
    │   (Vue.js)    │ │  (Flask)    │ │  (3000)     │
    └───────────────┘ └──────┬──────┘ └─────────────┘
                             │
    ┌────────────────────────┼────────────────────────┐
    │                        │                        │
┌───▼───┐ ┌─────────┐ ┌──────▼──────┐ ┌────────────┐ │
│ Redis │ │  MySQL  │ │ Celery      │ │ Prometheus │ │
│(6379) │ │ (3306)  │ │ Worker      │ │  (9090)    │ │
└───────┘ └─────────┘ └─────────────┘ └────────────┘ │
                                                    │
┌───────────────────────────────────────────────────┘
│
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  │  Filebeat   │───▶│    Kafka    │───▶│  Processor  │
│  │ (Collection)│    │   (Queue)   │    │ (Processing)│
│  └─────────────┘    └─────────────┘    └──────┬──────┘
│                                               │
└───────────────────────────────────────────────┘
                        │
                        ▼
                    ┌─────────┐
                    │  MySQL  │
                    │ (Store) │
                    └─────────┘
```

---

## Quick Start

### 1. Clone the Project

```bash
git clone https://github.com/322dfs/ObsCraft.git
cd ObsCraft
```

### 2. Configure Environment Variables

Edit `docker-compose.yaml` and modify the following configurations:

```yaml
# Email alert configuration
SMTP_USER: your_email@qq.com
SMTP_PASSWORD: your_authorization_code

# AI analysis configuration
DEEPSEEK_API_KEY: your_deepseek_api_key
```

### 3. Start Services

```bash
docker compose up -d
```

### 4. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Frontend | http://your-ip:8080/ | No login required |
| Grafana | http://your-ip:3000/ | admin / admin |
| Prometheus | http://your-ip:9090/ | No login required |

---

## Project Structure

```
ObsCraft/
├── celery-worker/          # Celery task processing
│   ├── Dockerfile
│   ├── requirements.txt
│   └── tasks.py
├── filebeat/               # Log collection
│   └── filebeat.yml
├── frontend/               # Frontend pages
│   ├── Dockerfile
│   └── public/
│       └── index.html
├── grafana/                # Grafana configuration
│   └── provisioning/
│       ├── dashboards/
│       │   ├── dashboard.json
│       │   └── dashboards.yml
│       └── datasources/
│           └── datasources.yml
├── log-processor/          # Log processor
│   ├── Dockerfile
│   ├── processor.py
│   └── requirements.txt
├── mysql-exporter/         # MySQL Exporter configuration
│   └── .my.cnf
├── nginx/                  # Nginx configuration
│   └── nginx.conf
├── prometheus/             # Prometheus configuration
│   └── prometheus.yml
├── web-app/                # Web application
│   ├── Dockerfile
│   ├── app.py
│   └── requirements.txt
├── docker-compose.yaml     # Docker orchestration file
├── README.md               # Documentation (Chinese)
├── README_EN.md            # Documentation (English)
├── 测试报告.md             # Test manual
└── 操作文档.md             # Operation guide
```

---

## Feature Demo

### Frontend Page

Visit http://your-ip:8080/ to see:

- **Dashboard**: System metrics and service status
- **Logs**: Log records from database
- **Alerts**: Alert records and AI analysis results

### Alert Functionality

Click the "Trigger 500" button, the system will:

1. Create an alert record
2. Call DeepSeek API for AI analysis
3. Send email alert (including AI analysis results)

### Monitoring Dashboard

Grafana dashboard includes:

- System uptime
- Prometheus memory usage
- Service status monitoring
- Redis/Kafka/Web application metrics

---

## Core Performance Metrics

| Metric | Value |
|--------|-------|
| Log collection latency | ≤ 10s |
| Alert response time | ≤ 30s |
| Dashboard refresh latency | ≤ 5s |
| Single node CPU usage | ≤ 10% |
| Single node memory usage | ≤ 2GB |

---

## Configuration Guide

### Email Alert Configuration

1. Login to QQ Mail → Settings → Account
2. Enable POP3/SMTP service
3. Generate authorization code
4. Configure SMTP_USER and SMTP_PASSWORD in docker-compose.yaml

### AI Analysis Configuration

1. Register DeepSeek account: https://platform.deepseek.com/
2. Get API Key
3. Configure DEEPSEEK_API_KEY in docker-compose.yaml

---

## Common Commands

```bash
# Start all services
docker compose up -d

# Check service status
docker ps

# View logs
docker logs log-web
docker logs log-processor
docker logs log-celery-worker

# Restart services
docker compose restart

# Stop all services
docker compose down
```

---

## Future Roadmap

| Version | Goal |
|---------|------|
| v4.0 | ELK integration, dual-link log retrieval |
| v5.0 | Kubernetes deployment support |
| v6.0 | Alert classification and intelligent thresholds |
| v7.0+ | AI enhancement: Natural language log query and fault diagnosis |

---

## Contributing

Issues and Pull Requests are welcome!

---

## License

MIT License

---

## Contact

- Email: 2080981057@qq.com
- GitHub: https://github.com/322dfs/ObsCraft
