# Distributed Systems Project

## 📘 Overview

This project was developed as part of the **Distributed Systems** course (Senior 1, Semester 8). It demonstrates key concepts of distributed computing, including communication, synchronization, fault tolerance, and scalability.

## ✨ Features

- Distributed architecture with multiple nodes
- Inter-node communication using **RabbitMQ** and **Amazon SQS**
- Fault tolerance mechanisms
- Data consistency and synchronization

## 🛠️ Technologies Used

- **Programming Language**: Python
- **Docker**
- **Kubernetes** (for scalable worker nodes on EC2 machines)
- **MongoDB Replica Set**
- **Elasticsearch**
- **Prometheus/Grafana** for monitoring

## 🚀 Usage

1. **Launch EC2 instances** for:
   - RabbitMQ
   - MongoDB nodes

   Connect via SSH and run:
   ```bash
   docker start mongo1 mongo2 queue_service
2. **Launch elastic search ec2s (3 nodes)**
3. **Deploy nodes**:
   - Master node
   - Worker nodes (scale as needed with kubernetes deployments)
   

## Files to view
These are the files used in the actual ec2 machines
new-master-node.py
crawler.py
indexer.py
mongo_db_node.py
server.py
google.html works as client interface


