FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY sy1.py .

# Create data directory
RUN mkdir -p /app/data

# Keep container running
CMD ["python", "sy1.py"]
