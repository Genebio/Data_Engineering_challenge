# Solution: Data Engineer position test challenge

## Overview
This report outlines the design and implementation of a data pipeline for IHC attribution service, developed as part of a technical challenge for a Data Engineering position.

## Pipeline Design

I designed a modular, scalable pipeline to process customer journey data through IHC attribution API. The architecture follows these key principles:

1. **Modularity**: Separated concerns into focused classes to handle:
   - Configuration management
   - Database operations
   - Customer journey construction
   - API communication
   - Channel reporting

2. **Scalability**: 
   - Containerized using Docker
   - Orchestrated via Airflow on Kubernetes
   - Implemented chunking for API requests to handle large datasets

3. **Configurability**:
   - Externalized all parameters to config files
   - Added time-range filtering for incremental processing

## Code Structure

The pipeline code is organized in the following key files:

```
pipeline/
├── __init__.py             # Package definition
├── config.py               # Configuration management with INI support
├── db_operations.py        # Database operations with context managers
├── cj_builder.py           # Customer journey building
├── api_client.py           # IHC API client with retry/chunking
├── channel_reporter.py     # Channel reporting and visualization
└── pipeline.py             # Main pipeline orchestration

config.ini                  # Configuration parameters (not included)
requirements.txt            # Python dependencies (not included)
Dockerfile                  # Container definition (not included)
run_pipeline.py             # CLI entry point
dags/attribution_pipeline_dag.py  # Airflow DAG
```

**Quick start guide:**
1. For local execution: `python run_pipeline.py`
2. For scheduled runs: Deploy the Airflow DAG

## Assumptions

- Event tracking is correctly implemented using both server and client-side collection (via Snowplow) to handle Intelligent Tracking Prevention (ITP)
- User-session data matching has been performed using Bayesian statistics/proprietary algorithms
- Source data quality is consistent and follows expected schemas

## Potential Improvements

1. **Technical Improvements**:
   - Comprehensive unit and integration testing suite
   - Enhanced monitoring and logging (Prometheus/Grafana)
   - Full CI/CD pipeline integration

2. **Functional Enhancements**:
   - Implement flag files or Redis to enable parallel processing of same-named dimensions
   - Develop a one-stop-shop API with OpenAPI specification that handles source data, builds journeys, trains parameters, and delivers results
   - Add batch processing capabilities allowing users to process multiple datasets in parallel
   
3. **User Experience**:
   - Create dashboard for monitoring pipeline health and attribution results
   - Develop self-service interface for marketing teams (especially features for time-series behavioral  data analysis)

## Conclusion

The pipeline effectively automates the process of extracting customer journey data, sending it to the IHC attribution API, and generating actionable channel reports. Its modular design enables easy maintenance and extension as business requirements evolve.