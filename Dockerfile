FROM python:3.11-slim
WORKDIR /app
COPY . .
ENV PYTHONDONTWRITEBYTECODE=1
CMD ["python", "-m", "unittest", "discover", "-s", "tests", "-v"]
