# Use uma imagem base leve
FROM python:3.12-slim

# Diretório de trabalho no container
WORKDIR /app

# Copia os arquivos necessários
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your script to the container
COPY main.py .

# Exponha a porta que o FastAPI usará
EXPOSE 8080

# PARA JOBS (Execute o script e saia):
CMD ["python", "main.py"]
