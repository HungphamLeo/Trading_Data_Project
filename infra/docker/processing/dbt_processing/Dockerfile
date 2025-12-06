FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements_dbt.txt .

# Install DBT and dependencies
RUN pip install --no-cache-dir -r requirements_dbt.txt

# Copy DBT project
COPY platform/processing/dbt/binance /app/dbt_project
COPY shared /app/shared

# Create directories
RUN mkdir -p /app/logs

# Set working directory to dbt project
WORKDIR /app/dbt_project

# Set Python path
ENV PYTHONPATH=/app
ENV DBT_PROFILES_DIR=/app/dbt_project

# Default command
CMD ["dbt", "run"]