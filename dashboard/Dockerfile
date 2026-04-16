FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir streamlit plotly pandas duckdb pymongo pyarrow
COPY app.py app.py
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0", "--server.port=8501"]
