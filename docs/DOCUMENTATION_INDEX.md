# StreamFlow Documentation Index

Welcome to the StreamFlow documentation! This index will help you find the right documentation for your needs.

---

## Quick Links

### 🚀 Getting Started
- [README](../README.md) - Quick start guide and project overview
- [Quick Start Example](../client/src/main/java/com/streamflow/client/example/QuickStart.java) - Working code example

### 📚 Learning Resources
- [Learning Guide](LEARNING_GUIDE.md) - Comprehensive educational guide (400+ lines)
- [Architecture](ARCHITECTURE.md) - System architecture and design

### 👨‍💻 For Developers
- [Developer Guide](DEVELOPER_GUIDE.md) - Development setup and workflow
- [API Reference](API_REFERENCE.md) - Complete API documentation
- [Project Structure](../PROJECT_STRUCTURE.md) - Directory layout and organization

### 🚢 For Operations
- [Deployment Guide](DEPLOYMENT_GUIDE.md) - Production deployment instructions
- [Docker Compose](../docker-compose.yml) - Containerized deployment

### 📊 Project Status
- [Implementation Status](../IMPLEMENTATION_COMPLETE.md) - What's been built
- [Project Structure](../PROJECT_STRUCTURE.md) - Complete file tree

---

## Documentation by Role

### I'm a Student / Learning Distributed Systems
**Start here**:
1. [README](../README.md) - Understand what StreamFlow is
2. [Learning Guide](LEARNING_GUIDE.md) - Deep dive into concepts
3. [Architecture](ARCHITECTURE.md) - How it all works together
4. [Phase 1: Storage Layer](phase1-storage-layer.md) - Log-structured storage
5. [Phase 2: Network Layer](phase2-network-layer.md) - Binary protocol
6. [Phase 3: Consumer Groups](phase3-consumer-groups.md) - Load balancing

**Try it out**:
- [Quick Start Example](../client/src/main/java/com/streamflow/client/example/QuickStart.java)
- [Developer Guide - Running the Application](DEVELOPER_GUIDE.md#running-the-application)

---

### I'm a Developer / Contributor
**Start here**:
1. [Developer Guide](DEVELOPER_GUIDE.md) - Setup your environment
2. [Project Structure](../PROJECT_STRUCTURE.md) - Understand the codebase
3. [Architecture](ARCHITECTURE.md) - System design
4. [API Reference](API_REFERENCE.md) - API contracts

**Building and Testing**:
- [Build Instructions](DEVELOPER_GUIDE.md#building-and-testing)
- [Adding New Features](DEVELOPER_GUIDE.md#adding-new-features)
- [Debugging](DEVELOPER_GUIDE.md#debugging)

**Code References**:
- [Implementation Status](../IMPLEMENTATION_COMPLETE.md#🗂️-project-structure)
- [Test Files](../broker/src/test/java/com/streamflow/broker/)

---

### I'm an Operator / DevOps Engineer
**Start here**:
1. [Deployment Guide](DEPLOYMENT_GUIDE.md) - Deploy to production
2. [Docker Deployment](DEPLOYMENT_GUIDE.md#docker-deployment) - Containerized setup
3. [Configuration](DEPLOYMENT_GUIDE.md#configuration) - Tuning parameters
4. [Monitoring Setup](DEPLOYMENT_GUIDE.md#monitoring-setup) - Prometheus + Grafana

**Operations**:
- [Backup and Recovery](DEPLOYMENT_GUIDE.md#backup-and-recovery)
- [Scaling](DEPLOYMENT_GUIDE.md#scaling)
- [Troubleshooting](DEPLOYMENT_GUIDE.md#troubleshooting)

**Quick Deploy**:
```bash
# Docker Compose (easiest)
docker-compose up -d

# Access services:
# - Broker: localhost:9092
# - Admin API: http://localhost:8080/api/swagger-ui.html
# - Prometheus: http://localhost:9090
# - Grafana: http://localhost:3000 (admin/admin)
```

---

### I'm Using the API
**Start here**:
1. [API Reference](API_REFERENCE.md) - Complete API documentation
2. [Admin REST API](API_REFERENCE.md#admin-rest-api) - HTTP endpoints
3. [Producer Client API](API_REFERENCE.md#producer-client-api) - Java producer
4. [Consumer Client API](API_REFERENCE.md#consumer-client-api) - Java consumer

**Examples**:
- [Quick Start](../client/src/main/java/com/streamflow/client/example/QuickStart.java)
- [REST API Examples](API_REFERENCE.md#examples)
- [Complete Producer Example](API_REFERENCE.md#complete-producer-example)
- [Complete Consumer Example](API_REFERENCE.md#complete-consumer-example)

**Swagger UI**:
```bash
# Start admin API
mvn spring-boot:run -f admin/pom.xml

# Open Swagger UI
open http://localhost:8080/api/swagger-ui.html
```

---

## Documentation by Topic

### Architecture & Design
| Document | Description |
|----------|-------------|
| [Architecture](ARCHITECTURE.md) | Complete system architecture |
| [Storage Architecture](ARCHITECTURE.md#storage-architecture) | Log-structured storage |
| [Network Protocol](ARCHITECTURE.md#network-protocol) | Binary protocol specification |
| [Replication](ARCHITECTURE.md#replication-architecture) | Leader-follower replication |
| [Consumer Groups](ARCHITECTURE.md#consumer-group-coordination) | Group coordination |

### Development
| Document | Description |
|----------|-------------|
| [Developer Guide](DEVELOPER_GUIDE.md) | Complete development guide |
| [Getting Started](DEVELOPER_GUIDE.md#getting-started) | Setup environment |
| [Building](DEVELOPER_GUIDE.md#building-and-testing) | Build and test |
| [Adding Features](DEVELOPER_GUIDE.md#adding-new-features) | Extend the platform |
| [Code Style](DEVELOPER_GUIDE.md#code-style-guidelines) | Coding conventions |

### Deployment
| Document | Description |
|----------|-------------|
| [Deployment Guide](DEPLOYMENT_GUIDE.md) | Complete deployment guide |
| [Local Deployment](DEPLOYMENT_GUIDE.md#local-deployment) | Run locally |
| [Docker Deployment](DEPLOYMENT_GUIDE.md#docker-deployment) | Containerized setup |
| [Production Deployment](DEPLOYMENT_GUIDE.md#production-deployment) | Production-ready setup |
| [Monitoring](DEPLOYMENT_GUIDE.md#monitoring-setup) | Prometheus + Grafana |

### API Documentation
| Document | Description |
|----------|-------------|
| [API Reference](API_REFERENCE.md) | Complete API reference |
| [REST API](API_REFERENCE.md#admin-rest-api) | Admin REST endpoints |
| [Binary Protocol](API_REFERENCE.md#binary-protocol-api) | Wire protocol |
| [Producer API](API_REFERENCE.md#producer-client-api) | Producer client |
| [Consumer API](API_REFERENCE.md#consumer-client-api) | Consumer client |
| [Error Codes](API_REFERENCE.md#error-codes) | All error codes |

### Learning
| Document | Description |
|----------|-------------|
| [Learning Guide](LEARNING_GUIDE.md) | Educational deep dive |
| [Phase 1: Storage](phase1-storage-layer.md) | Storage layer implementation |
| [Phase 2: Network](phase2-network-layer.md) | Network layer implementation |
| [Phase 3: Consumer Groups](phase3-consumer-groups.md) | Consumer groups |
| [Phase 5: Admin API](phase5-admin-api.md) | Admin API implementation |

---

## Complete File List

### Main Documentation (11 files)
```
docs/
├── DOCUMENTATION_INDEX.md           # This file
├── ARCHITECTURE.md                  # System architecture (600+ lines)
├── DEVELOPER_GUIDE.md               # Developer guide (800+ lines)
├── DEPLOYMENT_GUIDE.md              # Deployment guide (700+ lines)
├── API_REFERENCE.md                 # API reference (800+ lines)
├── LEARNING_GUIDE.md                # Learning guide (400+ lines)
├── phase1-storage-layer.md          # Phase 1 (300+ lines)
├── phase2-network-layer.md          # Phase 2 (300+ lines)
├── phase3-consumer-groups.md        # Phase 3 (200+ lines)
└── phase5-admin-api.md              # Phase 5 (300+ lines)
```

### Project Documentation (3 files)
```
├── README.md                        # Project overview
├── IMPLEMENTATION_COMPLETE.md       # Implementation status
└── PROJECT_STRUCTURE.md             # Directory structure
```

### Configuration (10 files)
```
├── pom.xml                          # Parent POM
├── common/pom.xml
├── broker/pom.xml
├── client/pom.xml
├── admin/pom.xml
├── Dockerfile.broker
├── Dockerfile.admin
├── docker-compose.yml
├── monitoring/prometheus.yml
└── admin/src/main/resources/application.yml
```

---

## Documentation Statistics

- **Total Documentation Files**: 11
- **Total Lines of Documentation**: ~4,700 lines
- **Total Project Files**: 78 files
- **Total Lines of Code**: ~7,500 lines
- **Documentation-to-Code Ratio**: 63%

---

## Quick Command Reference

### Build
```bash
mvn clean install              # Build all modules
mvn test                       # Run all tests
```

### Run Locally
```bash
# Terminal 1: Broker
java -jar broker/target/streamflow-broker-1.0.0-SNAPSHOT.jar

# Terminal 2: Admin API
java -jar admin/target/streamflow-admin-1.0.0-SNAPSHOT.jar
```

### Run with Docker
```bash
docker-compose up -d           # Start all services
docker-compose logs -f         # View logs
docker-compose down            # Stop all services
```

### Test APIs
```bash
# Create topic
curl -X POST http://localhost:8080/api/topics \
  -H "Content-Type: application/json" \
  -d '{"name":"test","partitions":3,"replicationFactor":1}'

# Get topic info
curl http://localhost:8080/api/topics/test | jq

# Health check
curl http://localhost:8080/api/broker/health | jq
```

---

## External Resources

### Related Technologies
- [Apache Kafka](https://kafka.apache.org/documentation/) - Inspiration
- [Netty](https://netty.io/wiki/index.html) - Network framework
- [Spring Boot](https://spring.io/projects/spring-boot) - Admin API framework
- [Prometheus](https://prometheus.io/docs/introduction/overview/) - Metrics
- [Grafana](https://grafana.com/docs/) - Visualization

### Learning Resources
- [Designing Data-Intensive Applications](https://dataintensive.net/) - Book
- [Kafka: The Definitive Guide](https://www.confluent.io/resources/kafka-the-definitive-guide/) - Book
- [Log-Structured Merge Tree](https://en.wikipedia.org/wiki/Log-structured_merge-tree) - Concept

---

## Contributing

We welcome contributions! See [Developer Guide - Adding New Features](DEVELOPER_GUIDE.md#adding-new-features)

---

## Support

- **Issues**: [Report bugs or request features](https://github.com/your-repo/issues)
- **Questions**: Check the documentation first, then ask on discussions
- **Documentation**: Found an error? Submit a PR!

---

**Last Updated**: 2026-03-15
**Version**: 1.0.0
**Status**: Complete (6/6 phases) ✅

Happy learning and building with StreamFlow! 🚀
