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
COPY requirements_ingestion.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements_ingestion.txt

# Copy application code
COPY platforms/ingestion /app/platform/ingestion
COPY shared /app/shared

# Create directories for logs and data storage
RUN mkdir -p /app/logs /app/logger_storage /app/output /app/temp_data_storage /app/scripts

# Set Python path
ENV PYTHONPATH=/app

# Default command - keep container running
CMD ["tail", "-f", "/dev/null"]
