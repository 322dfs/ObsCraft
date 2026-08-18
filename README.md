# ObsCraft - 基于 Kafka 的分布式日志收集与实时可观测性平台

## 项目简介

企业级日志收集与监控平台，实现从 Nginx Web 服务器日志采集、Kafka 消息队列缓冲、Python 消费者清洗入库、到 Celery 异步告警的全链路闭环，并通过 Prometheus + Grafana 构建完整可观测性体系。

## 系统架构

`
                    ┌─────────────────────────────────────────────────────────┐
                    │                    Nginx Load Balancer (:80)              │
                    │           轮询分发 → web-1 / web-2 / web-3               │
                    └──────────┬──────────────────┬──────────────────┬────────┘
                               │                  │                  │
                        access.log            access.log          access.log
                        error.log              error.log          error.log
                               │                  │                  │
                               └──────────┬───────┘                  │
                                          │                          │
                                   ┌──────▼──────┐                   │
                                   │   Filebeat  │                   │
                                   │  (日志采集)  │                   │
                                   └──────┬──────┘                   │
                                          │                          │
                              生产消息到 Kafka log-topic             │
                                          │                          │
                    ┌─────────────────────┼──────────────────┐      │
                    │                     │                  │      │
              ┌─────▼─────┐         ┌─────▼─────┐      ┌─────▼───┐  │
              │  Kafka-1  │◄───────►│  Kafka-2  │◄────►│ Kafka-3 │  │
              │ (KRaft)   │         │ (KRaft)   │      │ (KRaft) │  │
              └─────┬─────┘         └───────────┘      └─────────┘  │
                    │                                               │
                    │ 消费 (Consumer Group)                          │
              ┌─────▼─────────────┐                                 │
              │  Python Consumer   │                                │
              │  - 日志解析清洗     │                                │
              │  - 批量入库MySQL    │                                │
              │  - 检测4xx/5xx      │                                │
              └──┬──────────┬──────┘                                │
                 │          │                                       │
          ┌──────▼──┐  ┌────▼──────────┐                            │
          │  MySQL   │  │  Celery Worker │                            │
          │ (持久化) │  │  (异步告警)    │                            │
          └─────────┘  └───────┬───────┘                             │
                               │                                     │
                        ┌──────▼──────┐  ┌──────────┐               │
                        │    Redis     │  │  钉钉告警  │               │
                        │ (Broker)     │  └──────────┘               │
                        └─────────────┘  ┌──────────┐               │
                                         │  邮件告警  │               │
                                         └──────────┘               │

  ┌─────────────────────────────────────────────────────────────────┐
  │                    监控层 (Prometheus + Grafana)                  │
  │  node-exporter → 主机CPU/内存/磁盘/网络                           │
  │  kafka-exporter → Kafka集群健康/消息速率/Lag                     │
  │  mysqld-exporter → MySQL查询/连接数/慢查询                        │
  │  redis-exporter → Redis内存/连接/命令统计                         │
  │  cAdvisor → 所有容器的CPU/内存/网络/IO                            │
  └─────────────────────────────────────────────────────────────────┘
`

## 技术栈

| 组件 | 技术 | 版本 | 用途 |
|------|------|------|------|
| 负载均衡 | Nginx | 1.25-alpine | 3台Web服务器轮询负载均衡 |
| 日志采集 | Filebeat | 8.13.0 | 采集Nginx access/error日志，生产到Kafka |
| 消息队列 | Apache Kafka | latest (KRaft) | 3节点集群，6分区3副本，高可用缓冲 |
| 消费者 | Python | 3.9-slim | 多进程消费，正则解析，批量入库 |
| 数据库 | MySQL | 8.0 | 日志持久化存储，索引优化查询 |
| 异步任务 | Celery + Redis | 5.x + 7-alpine | 4xx/5xx错误实时告警（邮件+钉钉） |
| 监控采集 | Prometheus | latest | 6个exporter目标，15s采集间隔 |
| 可视化 | Grafana | 10.4-alpine | 6个预置仪表盘，自动数据源配置 |
| 容器编排 | Docker Compose | v2 | 18个容器一键编排 |

## 项目结构

`
kafka-log-platform/
├── docker-compose.yml          # 核心编排文件（18个容器）
├── app/                        # Python 应用
│   ├── Dockerfile              # Consumer + Celery Worker 共用镜像
│   ├── requirements.txt        # Python 依赖
│   ├── consumer.py             # Kafka消费者：解析→入库→触发告警
│   ├── tasks.py                # Celery告警任务：邮件+钉钉双通道
│   └── celery_app.py           # Celery实例配置（Redis as broker）
├── config/                     # 配置文件目录
│   ├── nginx/
│   │   ├── nginx-lb.conf       # 负载均衡配置（轮询3台Web）
│   │   ├── nginx-web.conf       # Web服务器配置
│   │   └── index.html          # 测试页面
│   ├── filebeat/
│   │   └── filebeat.yml        # 日志采集→Kafka输出配置
│   ├── mysql/
│   │   ├── init.sql            # 建库建表+监控用户初始化
│   │   ├── my.cnf              # MySQL主配置
│   │   └── exporter-my.cnf     # mysqld-exporter连接配置
│   ├── prometheus/
│   │   └── prometheus.yml      # 6个监控目标配置
│   └── grafana/
│       ├── datasource.yml      # Prometheus数据源自动配置
│       ├── dashboards.yml      # 仪表盘自动加载配置
│       └── dashboards/         # 6个预置仪表盘JSON
│           ├── host-overview.json      # 主机概览
│           ├── kafka-monitor.json       # Kafka集群监控
│           ├── mysql-monitor.json       # MySQL监控
│           ├── redis-monitor.json       # Redis监控
│           ├── container-overview.json  # 容器资源概览
│           └── app-overview.json        # 应用层综合概览
└── docs/                       # 项目文档
    ├── kafka-log-platform-doc.html  # 完整项目文档（含架构图/部署/量化数据）
    ├── assets/                      # 文档图表资源
    └── _shared/                     # 文档字体和JS库
`

## 快速开始

### 环境要求

- Docker Engine 24+
- Docker Compose v2+
- 服务器建议配置：2核 4GB+（低配云服务器可运行）

### 部署步骤

`ash
# 1. 克隆仓库
git clone https://github.com/322dfs/ObsCraft.git
cd ObsCraft

# 2. 启动全部服务（18个容器）
docker compose up -d

# 3. 查看运行状态
docker compose ps

# 4. 生成测试流量（触发日志采集）
for i in ; do curl http://localhost/; done

# 5. 查看消费入库情况
docker logs -f consumer

# 6. 访问 Grafana 仪表盘
# 浏览器打开 http://<服务器IP>:3000
# 默认账号 admin / admin
`

### 访问地址

| 服务 | 地址 | 说明 |
|------|------|------|
| Web 应用 | http://localhost:80 | Nginx 负载均衡 → 3台Web服务器 |
| Grafana | http://localhost:3000 | 6个预置仪表盘 |
| Prometheus | http://localhost:9090 | 指标采集与查询 |
| cAdvisor | http://localhost:8080 | 容器资源监控 |
| Kafka | kafka-1/2/3:9092 | 内部网络访问 |

## 核心设计

### Kafka 集群（KRaft 模式）

- **3 节点 KRaft 模式**：无需 ZooKeeper，减少运维复杂度和资源占用
- **6 分区 3 副本**：高并发写入 + 高可用容错，单节点故障不丢数据
- **Consumer Group**：log-consumer-group，手动提交 offset（enable_auto_commit=False），保证至少一次消费语义
- **Topic**：log-topic，auto-create 开启

### 日志处理链路

1. **Filebeat 采集**：挂载 3 台 Web 服务器的日志目录，通过 round-robin 生产到 Kafka 6 分区
2. **Consumer 消费**：拉取消息 → JSON 解析 → Nginx 正则提取字段 → 批量入库（BATCH_SIZE=100）
3. **错误告警**：检测到 4xx/5xx 状态码 → send_alert.delay() 异步触发 Celery → 邮件 + 钉钉双通道告警

### 监控体系

6 个 Prometheus 采集目标，15s 间隔：

| Exporter | 监控对象 | 关键指标 |
|----------|----------|----------|
| node-exporter | 主机系统 | CPU、内存、磁盘、网络 |
| kafka-exporter | Kafka 集群 | 消息速率、Lag、分区健康 |
| mysqld-exporter | MySQL | QPS、连接数、慢查询 |
| redis-exporter | Redis | 内存、连接、命令统计 |
| cAdvisor | 所有容器 | 容器 CPU/内存/网络/IO |
| Prometheus 自身 | 采集服务 | 采集延迟、规则评估 |

## 告警配置

告警通过环境变量注入敏感信息，支持双通道：

`ash
# 邮件告警（QQ邮箱 SMTP）
SMTP_HOST=smtp.qq.com
SMTP_PORT=465
SMTP_USER=your_qq@qq.com
SMTP_PASS=your_auth_code
TO_EMAIL=receiver@qq.com

# 钉钉告警（Webhook）
DINGTALK_WEBHOOK=https://oapi.dingtalk.com/robot/send?token=xxx
`

## 完整文档

详细的项目文档（含架构图、部署步骤、踩坑记录、量化数据、配置详解）请查看：

[项目文档](docs/kafka-log-platform-doc.html)

## License

MIT