"""Job de ETL: extrai leads/contatos de pipelines do Kommo e carrega no BigQuery.

Roda incrementalmente por `updated_at`: cada pipeline tem sua própria tabela
de destino em `bronze`, e o checkpoint é o próprio MAX(updated_at) já
carregado nessa tabela (sem depender de um controle externo de checkpoints).
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

load_dotenv()


class GCPJsonFormatter(logging.Formatter):
    """Formatador de log em JSON para o Google Cloud (Cloud Run)."""

    def format(self, record: logging.LogRecord) -> str:
        """Formata um LogRecord como uma linha JSON.

        Args:
            record: Registro de log gerado pelo logging.

        Returns:
            String JSON com severity/message/logger prontos pro Cloud Logging.
        """
        log_record = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log_record, ensure_ascii=False)


handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(GCPJsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
logger = logging.getLogger(__name__)

KOMMO_BASE_URL = os.getenv("KOMMO_BASE_URL", "https://boomfitclub.kommo.com")
KOMMO_API_KEY = os.getenv("KOMMO_API_KEY", "")

BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "acade-385821")
BQ_DATASET = os.getenv("BQ_DATASET", "bronze")

PAGE_SIZE = 250
REQUEST_DELAY_SECONDS = 0.2

# Schema explícito da tabela de destino: evita que o autodetect do BigQuery
# infira o tipo errado quando uma coluna vier inteira de nulos num batch
# (ex: closed_at quando nenhum lead do lote está fechado).
LEAD_SCHEMA = [
    bigquery.SchemaField("lead_id", "INTEGER"),
    bigquery.SchemaField("lead_name", "STRING"),
    bigquery.SchemaField("pipeline_id", "INTEGER"),
    bigquery.SchemaField("status_id", "INTEGER"),
    bigquery.SchemaField("price", "FLOAT"),
    bigquery.SchemaField("responsible_user_id", "INTEGER"),
    bigquery.SchemaField("is_deleted", "BOOLEAN"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("closed_at", "TIMESTAMP"),
    bigquery.SchemaField("contact_id", "INTEGER"),
    bigquery.SchemaField("contact_name", "STRING"),
    bigquery.SchemaField("contact_email", "STRING"),
    bigquery.SchemaField("contact_phone", "STRING"),
    bigquery.SchemaField("raw_json", "STRING"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]

# Pipelines do Kommo que devem ser sincronizados, com o nome da tabela de
# destino em bronze (raw_api_kommo_leads_<slug>).
PIPELINES = {
    "quiz": 14428987,
    "vendas_principal": 13056867,
    "seller": 13373175,
    "ativacao": 13574811,
    "indicados": 14271303,
    "churn_legado": 13574815,
    "churn_inadimplentes": 13872187,
    "churn_cancelados": 13872191,
}


def _kommo_get(path: str, params: dict) -> dict | None:
    """Faz uma requisição GET autenticada na API do Kommo.

    Args:
        path: Caminho da API (ex: "/api/v4/leads").
        params: Query params da requisição.

    Returns:
        Corpo da resposta já decodificado como dict, ou None se o Kommo
        retornar 204 (sem resultados).
    """
    url = f"{KOMMO_BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {KOMMO_API_KEY}"}

    for attempt in range(5):
        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code == 204:
            return None

        if response.status_code == 429:
            wait = 2**attempt
            logger.warning(f"Rate limit do Kommo, aguardando {wait}s...")
            time.sleep(wait)
            continue

        response.raise_for_status()
        time.sleep(REQUEST_DELAY_SECONDS)
        return response.json()

    raise RuntimeError(f"Falha ao chamar {url} após múltiplas tentativas.")


def fetch_leads(pipeline_id: int, updated_from: int | None) -> list[dict]:
    """Busca todos os leads de um pipeline, paginando até acabar.

    Args:
        pipeline_id: ID do pipeline no Kommo.
        updated_from: Timestamp unix a partir do qual buscar leads
            atualizados. None para buscar o histórico completo.

    Returns:
        Lista de leads (dicts brutos da API do Kommo).
    """
    leads: list[dict] = []
    page = 1

    while True:
        params = {
            "filter[pipeline_id]": pipeline_id,
            "with": "contacts",
            "limit": PAGE_SIZE,
            "page": page,
        }
        if updated_from is not None:
            params["filter[updated_at][from]"] = updated_from

        data = _kommo_get("/api/v4/leads", params)
        if not data:
            break

        page_leads = data.get("_embedded", {}).get("leads", [])
        if not page_leads:
            break

        leads.extend(page_leads)

        if len(page_leads) < PAGE_SIZE:
            break
        page += 1

    return leads


def fetch_contacts(contact_ids: list[int]) -> dict[int, dict]:
    """Busca detalhes (nome/telefone/email) de um conjunto de contatos.

    Args:
        contact_ids: IDs de contato a buscar.

    Returns:
        Dict mapeando contact_id -> dict com name/email/phone.
    """
    contacts: dict[int, dict] = {}
    unique_ids = sorted(set(contact_ids))

    for i in range(0, len(unique_ids), PAGE_SIZE):
        batch = unique_ids[i : i + PAGE_SIZE]
        params = {"filter[id][]": batch, "limit": PAGE_SIZE}
        data = _kommo_get("/api/v4/contacts", params)
        if not data:
            continue

        for contact in data.get("_embedded", {}).get("contacts", []):
            email = None
            phone = None
            for field in contact.get("custom_fields_values") or []:
                if field.get("field_code") == "EMAIL" and field.get("values"):
                    email = field["values"][0].get("value")
                if field.get("field_code") == "PHONE" and field.get("values"):
                    phone = field["values"][0].get("value")

            contacts[contact["id"]] = {
                "name": contact.get("name"),
                "email": email,
                "phone": phone,
            }

    return contacts


def _to_timestamp(value: int | None) -> pd.Timestamp | None:
    """Converte um timestamp unix (segundos) para Timestamp UTC do pandas.

    Args:
        value: Timestamp unix em segundos, ou None.

    Returns:
        pd.Timestamp em UTC, ou None se o valor de entrada for None/0.
    """
    if not value:
        return None
    return pd.Timestamp(datetime.fromtimestamp(value, tz=timezone.utc))


def extract(pipeline_id: int, updated_from: int | None) -> list[dict]:
    """Etapa 1: Extração. Busca leads e contatos de um pipeline do Kommo.

    Args:
        pipeline_id: ID do pipeline no Kommo.
        updated_from: Checkpoint (timestamp unix) a partir do qual buscar.

    Returns:
        Lista de leads brutos, cada um com a chave extra "_contact"
        contendo os dados do contato principal já resolvidos.
    """
    logger.info(f"--- 1. EXTRAÇÃO: pipeline {pipeline_id} (desde {updated_from}) ---")

    leads = fetch_leads(pipeline_id, updated_from)
    logger.info(f"Leads extraídos: {len(leads)}")

    contact_ids = []
    for lead in leads:
        for contact in lead.get("_embedded", {}).get("contacts", []):
            if contact.get("is_main"):
                contact_ids.append(contact["id"])

    contacts_map = fetch_contacts(contact_ids) if contact_ids else {}

    for lead in leads:
        main_contact_id = None
        for contact in lead.get("_embedded", {}).get("contacts", []):
            if contact.get("is_main"):
                main_contact_id = contact["id"]
                break
        lead["_contact"] = {"id": main_contact_id, **contacts_map.get(main_contact_id, {})}

    return leads


def transform(leads: list[dict]) -> pd.DataFrame:
    """Etapa 2: Transformação. Achata os leads brutos em um DataFrame.

    Args:
        leads: Leads brutos retornados por `extract`.

    Returns:
        DataFrame com uma linha por lead, pronto para carga no BigQuery.
    """
    logger.info("--- 2. TRANSFORMAÇÃO ---")

    rows = []
    for lead in leads:
        contact = lead.get("_contact") or {}
        rows.append(
            {
                "lead_id": lead["id"],
                "lead_name": lead.get("name"),
                "pipeline_id": lead.get("pipeline_id"),
                "status_id": lead.get("status_id"),
                "price": lead.get("price"),
                "responsible_user_id": lead.get("responsible_user_id"),
                "is_deleted": lead.get("is_deleted", False),
                "created_at": _to_timestamp(lead.get("created_at")),
                "updated_at": _to_timestamp(lead.get("updated_at")),
                "closed_at": _to_timestamp(lead.get("closed_at")),
                "contact_id": contact.get("id"),
                "contact_name": contact.get("name"),
                "contact_email": contact.get("email"),
                "contact_phone": contact.get("phone"),
                "raw_json": json.dumps(lead, ensure_ascii=False, default=str),
                "loaded_at": pd.Timestamp(datetime.now(tz=timezone.utc)),
            }
        )

    df = pd.DataFrame(rows)
    logger.info(f"Dados transformados: {len(df)} linhas.")
    return df


def get_checkpoint(client: bigquery.Client, table_id: str) -> int | None:
    """Lê o maior `updated_at` já carregado na tabela de destino.

    Args:
        client: Cliente do BigQuery já autenticado.
        table_id: Tabela de destino no formato `projeto.dataset.tabela`.

    Returns:
        Timestamp unix (segundos) do último `updated_at` carregado, ou
        None se a tabela ainda não existir (primeira carga/backfill).
    """
    try:
        client.get_table(table_id)
    except NotFound:
        return None

    query = f"SELECT UNIX_SECONDS(MAX(updated_at)) AS checkpoint FROM `{table_id}`"
    result = list(client.query(query).result())
    checkpoint = result[0]["checkpoint"] if result else None
    return int(checkpoint) if checkpoint is not None else None


def load(client: bigquery.Client, df: pd.DataFrame, table_name: str) -> None:
    """Etapa 3: Carga. Sobe o DataFrame pra staging e faz MERGE no destino.

    Args:
        client: Cliente do BigQuery já autenticado.
        df: DataFrame gerado por `transform`.
        table_name: Nome-base da tabela (sem o dataset), ex:
            "raw_api_kommo_leads_quiz".
    """
    logger.info("--- 3. CARGA ---")

    if df.empty:
        logger.info("Nenhuma linha nova/atualizada. Pulando carga.")
        return

    target_table = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"
    staging_table = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}_stg"

    job_config = bigquery.LoadJobConfig(
        schema=LEAD_SCHEMA, write_disposition="WRITE_TRUNCATE"
    )
    load_job = client.load_table_from_dataframe(df, staging_table, job_config=job_config)
    load_job.result()

    try:
        client.get_table(target_table)
    except NotFound:
        client.query(
            f"CREATE TABLE `{target_table}` AS SELECT * FROM `{staging_table}` WHERE FALSE"
        ).result()

    merge_query = f"""
        MERGE `{target_table}` AS target
        USING `{staging_table}` AS staging
        ON target.lead_id = staging.lead_id
        WHEN MATCHED THEN UPDATE SET
            lead_name = staging.lead_name,
            pipeline_id = staging.pipeline_id,
            status_id = staging.status_id,
            price = staging.price,
            responsible_user_id = staging.responsible_user_id,
            is_deleted = staging.is_deleted,
            created_at = staging.created_at,
            updated_at = staging.updated_at,
            closed_at = staging.closed_at,
            contact_id = staging.contact_id,
            contact_name = staging.contact_name,
            contact_email = staging.contact_email,
            contact_phone = staging.contact_phone,
            raw_json = staging.raw_json,
            loaded_at = staging.loaded_at
        WHEN NOT MATCHED THEN INSERT ROW
    """
    client.query(merge_query).result()

    logger.info(f"Carga concluída: {len(df)} linhas em {target_table}.")


def process_pipeline(client: bigquery.Client, slug: str, pipeline_id: int) -> None:
    """Orquestra extract/transform/load para um único pipeline do Kommo.

    Args:
        client: Cliente do BigQuery já autenticado.
        slug: Identificador curto do pipeline, usado no nome da tabela.
        pipeline_id: ID do pipeline no Kommo.
    """
    table_name = f"raw_api_kommo_leads_{slug}"
    target_table = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"

    logger.info(f"=== Pipeline '{slug}' ({pipeline_id}) -> {target_table} ===")

    checkpoint = get_checkpoint(client, target_table)
    leads = extract(pipeline_id, checkpoint)
    df = transform(leads)
    load(client, df, table_name)


def main() -> None:
    """Função principal que orquestra o ETL para todos os pipelines configurados."""
    if not KOMMO_API_KEY:
        raise RuntimeError("KOMMO_API_KEY não configurado.")

    client = bigquery.Client(project=BQ_PROJECT_ID)

    logger.info("Iniciando Job ETL Kommo -> BigQuery...")
    failures = []

    for slug, pipeline_id in PIPELINES.items():
        try:
            process_pipeline(client, slug, pipeline_id)
        except Exception as err:  # noqa: BLE001 (queremos seguir pros próximos pipelines)
            logger.error(f"Falha no pipeline '{slug}': {err}")
            failures.append(slug)

    if failures:
        raise RuntimeError(f"Job concluído com falhas nos pipelines: {failures}")

    logger.info("Job finalizado com sucesso! Encerrando container.")


if __name__ == "__main__":
    main()
