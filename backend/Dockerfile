FROM python:3.11-slim

# Build argument to choose PySpark variant
# Options:
#   - custom-local (default): Custom PySpark from local file, falls back to standard if not found
#   - custom-url: Custom PySpark downloaded from URL during build
#   - standard: Standard PySpark from PyPI
ARG PYSPARK_VARIANT=custom-local
ARG PYSPARK_VARIANT=standard

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

WORKDIR /app

# Copy requirements files
COPY requirements.txt requirements-custom-pyspark.txt ./

# Install dependencies based on PYSPARK_VARIANT
RUN if [ "$PYSPARK_VARIANT" = "custom-url" ]; then \
        echo "Installing custom PySpark from URL..."; \
        pip install --no-cache-dir -r requirements-custom-pyspark.txt; \
    elif [ "$PYSPARK_VARIANT" = "custom-local" ]; then \
        echo "Installing dependencies without PySpark (will install from local package at runtime)..."; \
        # Install all dependencies except PySpark
        grep -v "^pyspark" requirements.txt > /tmp/requirements-no-pyspark.txt && \
        pip install --no-cache-dir -r /tmp/requirements-no-pyspark.txt && \
        rm /tmp/requirements-no-pyspark.txt; \
    else \
        echo "Installing standard PySpark..."; \
        pip install --no-cache-dir -r requirements.txt; \
    fi

# Copy application code
COPY app/ /app/app/

# Copy entrypoint script
COPY docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Create directories for Spark, data, and local PySpark packages
RUN mkdir -p /app/spark /app/data /app/pyspark

# Set environment variable to indicate which variant is configured
ENV PYSPARK_VARIANT=$PYSPARK_VARIANT

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

# Use entrypoint script to handle custom-local PySpark installation
ENTRYPOINT ["/app/docker-entrypoint.sh"]

# Default command
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
