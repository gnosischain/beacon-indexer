FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files first
COPY . .

# Explicitly copy critical directories to ensure they're included
COPY migrations/ /app/migrations/
COPY scripts/ /app/scripts/

# Create additional directories
RUN mkdir -p /app/logs /app/state

# Copy state management scripts
COPY scripts/check_progress.py /app/scripts/
COPY scripts/manage_state.py /app/scripts/
COPY scripts/monitor_performance.py /app/scripts/

# Ensure all scripts are executable
RUN chmod +x /app/scripts/*.py 2>/dev/null || true
RUN chmod +x /app/scripts/*.sh 2>/dev/null || true

# Set entrypoint script
COPY scripts/docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

# Set default command
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["python", "-m", "src.main"]