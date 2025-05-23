# Use the official Python runtime as base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Set the working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code and config
COPY src/ ./src/
COPY config/ ./config/

# The PORT environment variable is automatically set by Cloud Functions
ENV PORT 8080

# Command to run the application using functions-framework
CMD exec functions-framework --target=cloud_function_entrypoint --source=src/main.py --port=$PORT
