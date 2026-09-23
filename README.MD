# Car Insurance Data Engineering Platform

## 📌 Introduction

This project is an **end-to-end Data Engineering project** built around a car insurance use case. The project demonstrates how data from multiple sources can be ingested, processed, transformed, and organized into a unified data platform using **Databricks**.

The system is designed to handle different types of data and ingestion patterns, including:

- **Streaming data** from Apache Kafka
- **Batch data** from PostgreSQL
- **Object and image data** from Amazon S3

Kafka and PostgreSQL are hosted on **AWS EC2**, while Databricks serves as the central platform for data ingestion, processing, transformation, and analytics.

The processed data follows the **Medallion Architecture**, moving through the **Bronze → Silver → Gold** layers. **Unity Catalog** is used as part of the Databricks data management and governance layer.

The resulting data platform provides a foundation for potential downstream use cases in the car insurance domain, such as:

- Insurance claim analytics
- Vehicle damage assessment from images
- Fraud detection

## 🎯 Purpose of the Project

The primary purpose of this project is **not to build a production-ready insurance system**, but to gain practical experience in designing and implementing an end-to-end Data Engineering platform with Databricks.

The project focuses on learning and experimenting with:

- Building an end-to-end data pipeline with **Databricks**
- Ingesting data from multiple heterogeneous sources
- Implementing **batch data ingestion** from PostgreSQL
- Implementing **real-time data streaming** with Apache Kafka
- Working with **object and image data** stored in Amazon S3
- Designing a **Medallion Architecture**
- Managing data with **Unity Catalog**
- Preparing data for downstream **Analytics and AI/ML workloads**
- Integrating different AWS services and data technologies into a unified Data Engineering architecture

Through this project, the goal is to understand not only how individual technologies work, but also **how they can be combined to build a complete data platform from data ingestion to downstream consumption**.

## 🏗️ Architecture

The overall architecture of the project is designed to ingest data from
multiple sources, process and transform it in Databricks using the
**Medallion Architecture**, and provide a foundation for downstream
analytics and AI/ML use cases.

![Project Architecture](./images/architecture/Car_Insurance_Architecture.drawio.png)

