import json
import logging
import sys

import pandas as pd
from dotenv import load_dotenv

# Carregar variáveis de ambiente do .env
load_dotenv()

class GCPJsonFormatter(logging.Formatter):
    """
    Formatador simples de log em JSON para o Google Cloud (Cloud Run).
    O Cloud Logging interpreta nativamente o campo 'severity' para a hierarquia
    de logs (DEBUG, INFO, WARNING, ERROR, CRITICAL) e 'message' como texto principal.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_record = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log_record, ensure_ascii=False)


# Configuração padrão de logging para stdout com suporte ao GCP
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(GCPJsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)

logger = logging.getLogger(__name__)


def extract():
    """Etapa 1: Extração.

    Simula buscar dados de uma fonte (CSV, API, Banco de Dados, etc.).
    """
    logger.info("--- 1. INICIANDO EXTRAÇÃO ---")

    # Dados fictícios para o exemplo
    raw_data = [
        {"id": 1, "item": "Notebook", "price": 2500.00, "currency": "BRL"},
        {"id": 2, "item": "Mouse", "price": 50.00, "currency": "BRL"},
        {"id": 3, "item": "Monitor", "price": 1200.00, "currency": "BRL"},
        {
            "id": 4,
            "item": "Cabo HDMI",
            "price": 20.00,
            "currency": "USD",
        },  # Moeda diferente
    ]

    df = pd.DataFrame(raw_data)
    logger.info(f"Dados extraídos: {len(df)} registros.")
    return df


def transform(df: pd.DataFrame):
    """Etapa 2: Transformação.

    Aplica regras de negócio, limpeza ou cálculos.
    """
    logger.info("--- 2. INICIANDO TRANSFORMAÇÃO ---")

    # Exemplo de regra: Converter tudo que é USD para BRL (taxa fixa de 5.0)
    taxa_dolar = 5.0

    # Cria uma nova coluna 'price_brl' normalizada
    df["price_normalized"] = df.apply(
        lambda row: row["price"] * taxa_dolar
        if row["currency"] == "USD"
        else row["price"],
        axis=1,
    )

    # Filtra apenas itens acima de 100 reais
    df_filtered = df[df["price_normalized"] > 100].copy()

    logger.info(f"Dados transformados. Registros restantes: {len(df_filtered)}")
    return df_filtered


def load(df: pd.DataFrame):
    """Etapa 3: Carga.

    Salva o resultado no destino (BigQuery, Cloud Storage, Banco SQL, etc.).
    """
    logger.info("--- 3. INICIANDO CARGA ---")

    # Num cenário real, aqui você usaria:
    # df.to_gbq(...) para BigQuery
    # df.to_csv('gs://meu-bucket/...') para Cloud Storage

    # Para este template simples, vamos apenas exibir o resultado final
    print("\nResultados Finais:")
    print(df.to_string(index=False))

    logger.info("Carga concluída com sucesso.")


def main():
    """Função principal que orquestra o Job ETL."""
    # Carrega variáveis de ambiente (útil para credenciais)
    load_dotenv()

    try:
        logger.info("Iniciando Job ETL...")

        data = extract()
        processed_data = transform(data)
        load(processed_data)

        logger.info("Job finalizado com sucesso! Encerrando container.")

    except Exception as e:
        logger.error(f"O Job falhou: {e}")
        # É importante levantar o erro para o Cloud Run saber que falhou e tentar novamente se configurado
        raise e


if __name__ == "__main__":
    main()
