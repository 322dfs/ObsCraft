# ObsCraft - 轻量级日志监控平台 v3.0

## 项目简介

ObsCraft 是一个轻量级分布式日志采集、实时监控与告警平台，专为Kafka集群设计。

### 核心功能

- **实时日志采集**: 通过Filebeat采集Nginx访问日志
- **日志处理与存储**: Kafka消息队列 + Python处理器 + MySQL存储
- **智能告警**: AI分析错误原因 + 邮件/钉钉告警通知
- **可视化监控**: Prometheus + Grafana 监控仪表板
- **一键部署**: Docker Compose 容器化部署

---

## 版本历史

| 版本 | 主要特性 |
|-----|---------|
| **v1.0** | Kafka集群(KRaft) + Filebeat + MySQL + Celery告警 |
| **v2.0** | 添加Prometheus监控 + Grafana可视化 |
| **v3.0** | Docker容器化 + AI智能分析 + 一键部署 |

---

## 技术栈

| 组件 | 版本/说明 |
|-----|---------|
| 前端 | Vue.js 3 + HTML5 |
| 后端 | Flask + Gunicorn |
| 消息队列 | Kafka + Zookeeper |
| 缓存 | Redis |
| 数据库 | MySQL 8.0 |
| 日志采集 | Filebeat |
| 监控 | Prometheus + Grafana |
| 任务队列 | Celery + Redis |
| AI分析 | DeepSeek API |
| 容器化 | Docker + Docker Compose |

---

## 系统架构

```
                    ┌─────────────────┐
                    │   用户浏览器     │
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
│  │ (日志采集)  │    │  (消息队列) │    │ (日志处理)  │
│  └─────────────┘    └─────────────┘    └──────┬──────┘
│                                               │
└───────────────────────────────────────────────┘
                        │
                        ▼
                    ┌─────────┐
                    │  MySQL  │
                    │ (存储)  │
                    └─────────┘
```

---

## 快速开始

### 1. 克隆项目

```bash
git clone https://github.com/322dfs/ObsCraft.git
cd ObsCraft
```

### 2. 配置环境变量

编辑 `docker-compose.yaml`，修改以下配置：

```yaml
# 邮箱告警配置
SMTP_USER: your_email@qq.com
SMTP_PASSWORD: your_authorization_code

# AI分析配置
DEEPSEEK_API_KEY: your_deepseek_api_key
```

### 3. 启动服务

```bash
docker compose up -d
```

### 4. 访问服务

| 服务 | 地址 | 账号 |
|-----|------|------|
| 前端页面 | http://your-ip:8080/ | 无需登录 |
| Grafana | http://your-ip:3000/ | admin / admin |
| Prometheus | http://your-ip:9090/ | 无需登录 |

---

## 项目结构

```
ObsCraft/
├── celery-worker/          # Celery任务处理
│   ├── Dockerfile
│   ├── requirements.txt
│   └── tasks.py
├── filebeat/               # 日志采集
│   └── filebeat.yml
├── frontend/               # 前端页面
│   ├── Dockerfile
│   └── public/
│       └── index.html
├── grafana/                # Grafana配置
│   └── provisioning/
│       ├── dashboards/
│       │   ├── dashboard.json
│       │   └── dashboards.yml
│       └── datasources/
│           └── datasources.yml
├── log-processor/          # 日志处理器
│   ├── Dockerfile
│   ├── processor.py
│   └── requirements.txt
├── mysql-exporter/         # MySQL Exporter配置
│   └── .my.cnf
├── nginx/                  # Nginx配置
│   └── nginx.conf
├── prometheus/             # Prometheus配置
│   └── prometheus.yml
├── web-app/                # Web应用
│   ├── Dockerfile
│   ├── app.py
│   └── requirements.txt
├── docker-compose.yaml     # Docker编排文件
├── README.md               # 项目说明(中文)
├── README_EN.md            # 项目说明(英文)
├── 测试报告.md             # 测试手册
└── 操作文档.md             # 操作说明
```

---

## 功能演示

### 前端页面

访问 http://your-ip:8080/ 可以看到：

- **首页**: 系统指标和服务状态
- **日志页**: 数据库中的日志记录
- **告警页**: 告警记录和AI分析结果

### 告警功能

点击"触发500"按钮，系统会：

1. 创建告警记录
2. 调用DeepSeek API进行AI分析
3. 发送邮件告警（包含AI分析结果）

### 监控仪表板

Grafana仪表板包含：

- 系统运行时间
- Prometheus内存使用
- 服务状态监控
- Redis/Kafka/Web应用指标

---

## 核心性能指标

| 指标 | 数值 |
|-----|------|
| 日志采集延迟 | ≤ 10s |
| 告警响应时间 | ≤ 30s |
| 仪表板刷新延迟 | ≤ 5s |
| 单节点CPU使用率 | ≤ 10% |
| 单节点内存使用 | ≤ 2GB |

---

## 配置说明

### 邮件告警配置

1. 登录QQ邮箱 → 设置 → 账户
2. 开启POP3/SMTP服务
3. 生成授权码
4. 在docker-compose.yaml中配置SMTP_USER和SMTP_PASSWORD

### AI分析配置

1. 注册DeepSeek账号: https://platform.deepseek.com/
2. 获取API Key
3. 在docker-compose.yaml中配置DEEPSEEK_API_KEY

---

## 常用命令

```bash
# 启动所有服务
docker compose up -d

# 查看服务状态
docker ps

# 查看日志
docker logs log-web
docker logs log-processor
docker logs log-celery-worker

# 重启服务
docker compose restart

# 停止所有服务
docker compose down
```

---

## 未来规划

| 版本 | 目标 |
|-----|------|
| v4.0 | ELK集成，双链路日志检索 |
| v5.0 | Kubernetes部署支持 |
| v6.0 | 告警分级与智能阈值 |
| v7.0+ | AI增强：自然语言日志查询与故障诊断 |

---

## 贡献指南

欢迎提交Issue和Pull Request！

---

## 许可证

MIT License

---

## 联系方式

- Email: 2080981057@qq.com
- GitHub: https://github.com/322dfs/ObsCraft
