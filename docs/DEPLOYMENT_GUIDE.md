# StreamFlow Deployment Guide

## Table of Contents
1. [Deployment Options](#deployment-options)
2. [Local Deployment](#local-deployment)
3. [Docker Deployment](#docker-deployment)
4. [Production Deployment](#production-deployment)
5. [Configuration](#configuration)
6. [Monitoring Setup](#monitoring-setup)
7. [Backup and Recovery](#backup-and-recovery)
8. [Scaling](#scaling)
9. [Troubleshooting](#troubleshooting)

---

## Deployment Options

### Option 1: Local Development
- Single broker on localhost
- Ideal for: Development, testing, learning
- Requirements: Java 17, Maven

### Option 2: Docker (Single Host)
- Broker + Admin + Prometheus + Grafana in containers
- Ideal for: Testing, demos, small deployments
- Requirements: Docker, Docker Compose

### Option 3: Production (Multi-Host)
- Multiple brokers across hosts
- Ideal for: Production workloads
- Requirements: Multiple servers, load balancer, monitoring

---

## Local Deployment

### Step 1: Build Project

```bash
# Clone repository
git clone <repository-url>
cd Project2_Event_Streaming_Platform

# Build all modules
mvn clean install

# Expected output:
# [INFO] ------------------------------------------------------------------------
# [INFO] Reactor Summary:
# [INFO] StreamFlow Parent .................................. SUCCESS
# [INFO] StreamFlow Common .................................. SUCCESS
# [INFO] StreamFlow Broker .................................. SUCCESS
# [INFO] StreamFlow Client .................................. SUCCESS
# [INFO] StreamFlow Admin ................................... SUCCESS
# [INFO] ------------------------------------------------------------------------
```

### Step 2: Start Broker

```bash
# Method 1: From Maven
cd broker
mvn exec:java -Dexec.mainClass="com.streamflow.broker.BrokerApplication"

# Method 2: From JAR
java -jar broker/target/streamflow-broker-1.0.0-SNAPSHOT.jar

# Method 3: With custom configuration
java -jar broker/target/streamflow-broker-1.0.0-SNAPSHOT.jar \
  --broker-id 0 \
  --host localhost \
  --port 9092 \
  --data-dir ./streamflow-data
```

Expected output:
```
INFO  BrokerApplication - Starting StreamFlow Broker...
INFO  TopicManager - TopicManager initialized
INFO  BrokerServer - Broker server started on port 9092
```

### Step 3: Start Admin API (Optional)

```bash
# Terminal 2
cd admin
mvn spring-boot:run

# Or from JAR
java -jar admin/target/streamflow-admin-1.0.0-SNAPSHOT.jar
```

Expected output:
```
INFO  AdminApplication - Started AdminApplication in 3.45 seconds
INFO  Tomcat - Tomcat started on port(s): 8080 (http)
```

### Step 4: Verify Deployment

```bash
# Check broker connectivity
telnet localhost 9092

# Check admin API
curl http://localhost:8080/api/actuator/health

# Expected response:
# {"status":"UP"}

# Access Swagger UI
open http://localhost:8080/api/swagger-ui.html
```

### Step 5: Test with Client

```java
// ProducerTest.java
public class ProducerTest {
    public static void main(String[] args) throws Exception {
        StreamFlowProducer producer = new StreamFlowProducer("localhost", 9092);
        producer.connect();

        for (int i = 0; i < 10; i++) {
            RecordMetadata metadata = producer.send("test-topic", "key-" + i, "message-" + i);
            System.out.println("Sent message to offset: " + metadata.offset());
        }

        producer.close();
    }
}
```

```bash
javac -cp client/target/streamflow-client-1.0.0-SNAPSHOT.jar ProducerTest.java
java -cp .:client/target/streamflow-client-1.0.0-SNAPSHOT.jar ProducerTest
```

---

## Docker Deployment

### Prerequisites

```bash
# Install Docker
# macOS: Download Docker Desktop
# Linux: sudo apt-get install docker docker-compose
# Verify installation
docker --version
docker-compose --version
```

### Step 1: Build Docker Images

```bash
# Build all modules
mvn clean package -DskipTests

# Build Docker images
docker-compose build

# Verify images
docker images | grep streamflow
# Expected output:
# streamflow-broker    latest    abc123...
# streamflow-admin     latest    def456...
```

### Step 2: Start Services

```bash
# Start all services in background
docker-compose up -d

# View logs
docker-compose logs -f

# Check running containers
docker ps
# Expected output:
# CONTAINER ID   IMAGE                 STATUS          PORTS
# abc123...      streamflow-broker     Up 10 seconds   0.0.0.0:9092->9092/tcp
# def456...      streamflow-admin      Up 10 seconds   0.0.0.0:8080->8080/tcp
# ghi789...      prom/prometheus       Up 10 seconds   0.0.0.0:9090->9090/tcp
# jkl012...      grafana/grafana       Up 10 seconds   0.0.0.0:3000->3000/tcp
```

### Step 3: Verify Services

```bash
# Check broker health
curl http://localhost:8080/api/broker/health

# Access web interfaces
open http://localhost:8080/api/swagger-ui.html    # Admin API
open http://localhost:9090                         # Prometheus
open http://localhost:3000                         # Grafana (admin/admin)
```

### Step 4: Service Configuration

**Environment Variables** (in `docker-compose.yml`):
```yaml
environment:
  - BROKER_ID=0
  - HOST=0.0.0.0
  - PORT=9092
  - DATA_DIR=/app/data
  - JAVA_OPTS=-Xmx512m -Xms256m
```

**Volume Mounts**:
```yaml
volumes:
  - broker-data:/app/data              # Persistent storage
  - ./logs:/app/logs                   # Log files
  - ./config:/app/config               # Custom config
```

### Step 5: Stop and Cleanup

```bash
# Stop services
docker-compose stop

# Stop and remove containers
docker-compose down

# Remove volumes (WARNING: deletes data)
docker-compose down -v

# Remove images
docker rmi streamflow-broker streamflow-admin
```

---

## Production Deployment

### Architecture

```
                    Load Balancer
                    (HAProxy/Nginx)
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
         ▼                 ▼                 ▼
    ┌─────────┐      ┌─────────┐      ┌─────────┐
    │Broker 0 │      │Broker 1 │      │Broker 2 │
    │Port 9092│      │Port 9092│      │Port 9092│
    └─────────┘      └─────────┘      └─────────┘
         │                 │                 │
         └─────────────────┴─────────────────┘
                           │
                  Shared Storage / NFS
                     (or local disks)
```

### Hardware Requirements

**Minimum (Per Broker)**:
- CPU: 4 cores
- RAM: 8 GB
- Disk: 500 GB SSD
- Network: 1 Gbps

**Recommended (Per Broker)**:
- CPU: 8-16 cores
- RAM: 32-64 GB
- Disk: 1-2 TB NVMe SSD
- Network: 10 Gbps

### Step 1: Prepare Servers

```bash
# Update system
sudo apt-get update && sudo apt-get upgrade -y

# Install Java 17
sudo apt-get install openjdk-17-jdk -y
java -version

# Create streamflow user
sudo useradd -r -s /bin/false streamflow
sudo mkdir -p /opt/streamflow
sudo mkdir -p /var/streamflow/data
sudo chown -R streamflow:streamflow /opt/streamflow /var/streamflow
```

### Step 2: Deploy Broker

```bash
# Copy JAR to server
scp broker/target/streamflow-broker-1.0.0-SNAPSHOT.jar \
    user@broker-host:/opt/streamflow/broker.jar

# Create systemd service
sudo cat > /etc/systemd/system/streamflow-broker.service <<EOF
[Unit]
Description=StreamFlow Broker
After=network.target

[Service]
Type=simple
User=streamflow
ExecStart=/usr/bin/java \
    -Xmx4g -Xms4g \
    -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=20 \
    -XX:InitiatingHeapOccupancyPercent=35 \
    -XX:+DisableExplicitGC \
    -Djava.awt.headless=true \
    -jar /opt/streamflow/broker.jar \
    --broker-id=\${BROKER_ID} \
    --host=\${HOST} \
    --port=9092 \
    --data-dir=/var/streamflow/data
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Set broker ID
echo "BROKER_ID=0" | sudo tee -a /etc/environment
echo "HOST=$(hostname -I | awk '{print $1}')" | sudo tee -a /etc/environment

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable streamflow-broker
sudo systemctl start streamflow-broker

# Check status
sudo systemctl status streamflow-broker

# View logs
sudo journalctl -u streamflow-broker -f
```

### Step 3: Deploy Admin API

```bash
# Copy JAR
scp admin/target/streamflow-admin-1.0.0-SNAPSHOT.jar \
    user@admin-host:/opt/streamflow/admin.jar

# Create systemd service
sudo cat > /etc/systemd/system/streamflow-admin.service <<EOF
[Unit]
Description=StreamFlow Admin API
After=network.target

[Service]
Type=simple
User=streamflow
ExecStart=/usr/bin/java \
    -Xmx2g -Xms2g \
    -jar /opt/streamflow/admin.jar
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Start service
sudo systemctl enable streamflow-admin
sudo systemctl start streamflow-admin
```

### Step 4: Configure Load Balancer

**HAProxy Configuration** (`/etc/haproxy/haproxy.cfg`):
```
frontend streamflow_brokers
    bind *:9092
    mode tcp
    default_backend streamflow_broker_cluster

backend streamflow_broker_cluster
    mode tcp
    balance roundrobin
    server broker0 10.0.1.10:9092 check
    server broker1 10.0.1.11:9092 check
    server broker2 10.0.1.12:9092 check

frontend streamflow_admin
    bind *:8080
    mode http
    default_backend streamflow_admin_servers

backend streamflow_admin_servers
    mode http
    balance roundrobin
    option httpchk GET /api/actuator/health
    server admin0 10.0.1.20:8080 check
```

Restart HAProxy:
```bash
sudo systemctl restart haproxy
```

### Step 5: Firewall Configuration

```bash
# Allow broker port
sudo ufw allow 9092/tcp

# Allow admin API port
sudo ufw allow 8080/tcp

# Allow Prometheus
sudo ufw allow 9090/tcp

# Allow Grafana
sudo ufw allow 3000/tcp

# Enable firewall
sudo ufw enable
```

---

## Configuration

### Broker Configuration

**File**: `broker/src/main/resources/application.properties`

```properties
# Broker Identity
broker.id=0
broker.host=localhost
broker.port=9092

# Storage
broker.data.dir=/var/streamflow/data
broker.log.segment.bytes=104857600        # 100MB
broker.log.retention.hours=168            # 7 days

# Replication
broker.replication.factor=3
broker.min.insync.replicas=2

# Network
broker.socket.send.buffer.bytes=102400
broker.socket.receive.buffer.bytes=102400
broker.max.message.bytes=1048576          # 1MB

# Performance
broker.num.network.threads=8
broker.num.io.threads=8
```

### Admin API Configuration

**File**: `admin/src/main/resources/application.yml`

```yaml
server:
  port: 8080
  servlet:
    context-path: /api

streamflow:
  broker:
    host: localhost
    port: 9092
    connection-timeout: 5000

management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
  metrics:
    export:
      prometheus:
        enabled: true

logging:
  level:
    root: INFO
    com.streamflow: DEBUG
  file:
    name: /var/log/streamflow/admin.log
    max-size: 10MB
    max-history: 30
```

### JVM Tuning

**Production JVM Options**:
```bash
java \
  -Xmx6g \                          # Max heap: 6GB
  -Xms6g \                          # Initial heap: 6GB
  -XX:+UseG1GC \                    # Use G1 garbage collector
  -XX:MaxGCPauseMillis=20 \         # Max GC pause: 20ms
  -XX:InitiatingHeapOccupancyPercent=35 \
  -XX:+DisableExplicitGC \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=/var/log/streamflow/heapdump.hprof \
  -Xlog:gc*:file=/var/log/streamflow/gc.log:time,uptime:filecount=10,filesize=100M \
  -jar broker.jar
```

---

## Monitoring Setup

### Prometheus Setup

**Install Prometheus**:
```bash
# Download
wget https://github.com/prometheus/prometheus/releases/download/v2.40.0/prometheus-2.40.0.linux-amd64.tar.gz
tar xvf prometheus-2.40.0.linux-amd64.tar.gz
cd prometheus-2.40.0.linux-amd64

# Configure
cat > prometheus.yml <<EOF
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'streamflow-admin'
    metrics_path: '/api/actuator/prometheus'
    static_configs:
      - targets: ['localhost:8080']
EOF

# Start
./prometheus --config.file=prometheus.yml
```

### Grafana Setup

**Install Grafana**:
```bash
sudo apt-get install -y software-properties-common
sudo add-apt-repository "deb https://packages.grafana.com/oss/deb stable main"
wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add -
sudo apt-get update
sudo apt-get install grafana

# Start
sudo systemctl start grafana-server
sudo systemctl enable grafana-server
```

**Access Grafana**:
1. Open http://localhost:3000
2. Login: admin/admin
3. Add Prometheus data source: http://localhost:9090
4. Create dashboard with panels:
   - Messages per second
   - Topics count
   - Partitions count
   - Consumer lag

### Alerts

**Prometheus Alert Rules** (`alerts.yml`):
```yaml
groups:
  - name: streamflow_alerts
    rules:
      - alert: BrokerDown
        expr: up{job="streamflow-admin"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "StreamFlow broker is down"

      - alert: HighConsumerLag
        expr: streamflow_consumer_lag > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High consumer lag detected"
```

---

## Backup and Recovery

### Backup Strategy

**Automated Backup Script**:
```bash
#!/bin/bash
# backup.sh

BACKUP_DIR="/backups/streamflow"
DATA_DIR="/var/streamflow/data"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create backup directory
mkdir -p $BACKUP_DIR

# Stop broker (optional, for consistent backup)
sudo systemctl stop streamflow-broker

# Backup data
tar -czf $BACKUP_DIR/streamflow-backup-$TIMESTAMP.tar.gz \
    -C $DATA_DIR .

# Restart broker
sudo systemctl start streamflow-broker

# Delete backups older than 7 days
find $BACKUP_DIR -name "streamflow-backup-*.tar.gz" -mtime +7 -delete

echo "Backup completed: $BACKUP_DIR/streamflow-backup-$TIMESTAMP.tar.gz"
```

**Schedule with cron**:
```bash
# Daily backup at 2 AM
0 2 * * * /opt/streamflow/backup.sh >> /var/log/streamflow/backup.log 2>&1
```

### Recovery

```bash
# Stop broker
sudo systemctl stop streamflow-broker

# Clear current data
sudo rm -rf /var/streamflow/data/*

# Restore from backup
sudo tar -xzf /backups/streamflow/streamflow-backup-20260315_020000.tar.gz \
    -C /var/streamflow/data

# Fix permissions
sudo chown -R streamflow:streamflow /var/streamflow/data

# Start broker
sudo systemctl start streamflow-broker
```

---

## Scaling

### Vertical Scaling

**Increase Resources**:
1. Stop broker
2. Upgrade server (more CPU, RAM, disk)
3. Adjust JVM heap size
4. Restart broker

```bash
# Update JVM options in systemd service
ExecStart=/usr/bin/java \
    -Xmx16g -Xms16g \    # Increased from 4GB to 16GB
    -jar /opt/streamflow/broker.jar
```

### Horizontal Scaling

**Add More Brokers**:

1. **Deploy new broker**:
   ```bash
   # On new server
   sudo systemctl start streamflow-broker
   ```

2. **Update load balancer**:
   ```
   server broker3 10.0.1.13:9092 check
   ```

3. **Rebalance partitions**:
   - Currently manual (future enhancement)
   - Would need partition reassignment tool

---

## Troubleshooting

### Common Issues

**Issue: Broker won't start**
```bash
# Check logs
sudo journalctl -u streamflow-broker -n 100

# Check port
sudo netstat -tulpn | grep 9092

# Check permissions
ls -la /var/streamflow/data
```

**Issue: Out of disk space**
```bash
# Check disk usage
df -h /var/streamflow

# Clean old segments (configure retention)
# Or manually delete old data
sudo rm -rf /var/streamflow/data/topics/old-topic
```

**Issue: High memory usage**
```bash
# Check heap usage
jmap -heap $(pgrep -f streamflow-broker)

# Dump heap
jmap -dump:format=b,file=heap.bin $(pgrep -f streamflow-broker)

# Analyze with VisualVM or Eclipse MAT
```

**Issue: Slow performance**
```bash
# Check GC logs
grep "GC" /var/log/streamflow/gc.log

# Monitor system resources
top
iostat -x 1
```

---

## Health Checks

### Automated Health Check Script

```bash
#!/bin/bash
# health-check.sh

# Check broker connectivity
if ! nc -z localhost 9092; then
    echo "ERROR: Broker not responding on port 9092"
    exit 1
fi

# Check admin API
if ! curl -sf http://localhost:8080/api/actuator/health > /dev/null; then
    echo "ERROR: Admin API health check failed"
    exit 1
fi

# Check disk space
DISK_USAGE=$(df /var/streamflow | tail -1 | awk '{print $5}' | sed 's/%//')
if [ $DISK_USAGE -gt 90 ]; then
    echo "WARNING: Disk usage is ${DISK_USAGE}%"
fi

echo "OK: All health checks passed"
```

**Run from cron every 5 minutes**:
```bash
*/5 * * * * /opt/streamflow/health-check.sh >> /var/log/streamflow/health.log 2>&1
```

---

For more information:
- **Architecture**: [ARCHITECTURE.md](ARCHITECTURE.md)
- **Development**: [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md)
- **API Reference**: [API_REFERENCE.md](API_REFERENCE.md)
