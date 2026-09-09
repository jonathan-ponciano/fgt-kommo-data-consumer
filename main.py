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

# Tabela única (não é por pipeline) com o histórico bruto de eventos de leads
# (troca de status, mensagens de chat, tags, etc). A API de eventos do Kommo
# é global à conta, então um único job cobre leads de todos os pipelines.
EVENTS_TABLE = "raw_api_kommo_events"

EVENT_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING"),
    bigquery.SchemaField("event_type", "STRING"),
    bigquery.SchemaField("entity_id", "INTEGER"),
    bigquery.SchemaField("entity_type", "STRING"),
    bigquery.SchemaField("created_by", "INTEGER"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("value_before", "STRING"),
    bigquery.SchemaField("value_after", "STRING"),
    bigquery.SchemaField("raw_json", "STRING"),
    bigquery.SchemaField("loaded_at", "TIMESTAMP"),
]

# Bug conhecido da API do Kommo: `filter[entity]=lead` exclui silenciosamente
# eventos de mensagem/ligação, mesmo eles tendo entity_type "lead" no
# payload. Por isso esses tipos precisam de uma segunda chamada, filtrando
# por `type` em vez de `entity`.
MESSAGE_EVENT_TYPES = [
    "incoming_chat_message",
    "outgoing_chat_message",
    "incoming_call",
    "outgoing_call",
]


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


def _fetch_events_paginated(base_params: dict, created_from: int | None) -> list[dict]:
    """Pagina `/api/v4/events` até acabar, com os filtros base informados.

    Args:
        base_params: Filtros fixos da chamada (ex: `filter[entity]` ou
            `filter[type][]`), sem paginação nem `created_at`.
        created_from: Timestamp unix a partir do qual buscar eventos
            criados. None para buscar o histórico completo.

    Returns:
        Lista de eventos (dicts brutos da API do Kommo).
    """
    events: list[dict] = []
    page = 1

    while True:
        params = {**base_params, "limit": PAGE_SIZE, "page": page}
        if created_from is not None:
            params["filter[created_at][from]"] = created_from

        data = _kommo_get("/api/v4/events", params)
        if not data:
            break

        page_events = data.get("_embedded", {}).get("events", [])
        if not page_events:
            break

        events.extend(page_events)

        if len(page_events) < PAGE_SIZE:
            break
        page += 1

    return events


def fetch_events(created_from: int | None) -> list[dict]:
    """Busca todos os eventos de leads da conta, paginando até acabar.

    Não é filtrado por pipeline: a API de eventos do Kommo é global à
    conta, então esse único feed cobre leads de todos os pipelines.

    Faz duas passadas: `filter[entity]=lead` pega a maioria dos tipos de
    evento, mas exclui silenciosamente mensagens/ligações (bug conhecido
    da API do Kommo) — por isso `MESSAGE_EVENT_TYPES` é buscado à parte,
    filtrando por `type`. O resultado é deduplicado por `id`.

    Args:
        created_from: Timestamp unix a partir do qual buscar eventos
            criados. None para buscar o histórico completo.

    Returns:
        Lista de eventos (dicts brutos da API do Kommo), sem duplicatas.
    """
    entity_events = _fetch_events_paginated({"filter[entity]": "lead"}, created_from)
    message_events = _fetch_events_paginated(
        {"filter[type][]": MESSAGE_EVENT_TYPES}, created_from
    )

    by_id = {event["id"]: event for event in entity_events + message_events}
    return list(by_id.values())


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


def extract_events(created_from: int | None) -> list[dict]:
    """Etapa 1: Extração. Busca eventos de leads (todos os pipelines).

    Args:
        created_from: Checkpoint (timestamp unix) a partir do qual buscar.

    Returns:
        Lista de eventos brutos retornados pelo Kommo.
    """
    logger.info(f"--- 1. EXTRAÇÃO DE EVENTOS (desde {created_from}) ---")
    events = fetch_events(created_from)
    logger.info(f"Eventos extraídos: {len(events)}")
    return events


def transform_events(events: list[dict]) -> pd.DataFrame:
    """Etapa 2: Transformação. Achata os eventos brutos em um DataFrame.

    Args:
        events: Eventos brutos retornados por `extract_events`.

    Returns:
        DataFrame com uma linha por evento, pronto para carga no BigQuery.
    """
    logger.info("--- 2. TRANSFORMAÇÃO DE EVENTOS ---")

    rows = []
    for event in events:
        rows.append(
            {
                "event_id": event["id"],
                "event_type": event.get("type"),
                "entity_id": event.get("entity_id"),
                "entity_type": event.get("entity_type"),
                "created_by": event.get("created_by"),
                "created_at": _to_timestamp(event.get("created_at")),
                "value_before": json.dumps(
                    event.get("value_before"), ensure_ascii=False, default=str
                ),
                "value_after": json.dumps(
                    event.get("value_after"), ensure_ascii=False, default=str
                ),
                "raw_json": json.dumps(event, ensure_ascii=False, default=str),
                "loaded_at": pd.Timestamp(datetime.now(tz=timezone.utc)),
            }
        )

    df = pd.DataFrame(rows)
    logger.info(f"Dados transformados: {len(df)} linhas.")
    return df


def get_checkpoint(
    client: bigquery.Client, table_id: str, timestamp_column: str
) -> int | None:
    """Lê o maior timestamp já carregado na tabela de destino.

    Args:
        client: Cliente do BigQuery já autenticado.
        table_id: Tabela de destino no formato `projeto.dataset.tabela`.
        timestamp_column: Coluna TIMESTAMP usada como checkpoint
            (ex: "updated_at" para leads, "created_at" para eventos).

    Returns:
        Timestamp unix (segundos) do último valor carregado, ou None se a
        tabela ainda não existir (primeira carga/backfill).
    """
    try:
        client.get_table(table_id)
    except NotFound:
        return None

    query = (
        f"SELECT UNIX_SECONDS(MAX({timestamp_column})) AS checkpoint "
        f"FROM `{table_id}`"
    )
    result = list(client.query(query).result())
    checkpoint = result[0]["checkpoint"] if result else None
    return int(checkpoint) if checkpoint is not None else None


def load(
    client: bigquery.Client,
    df: pd.DataFrame,
    table_name: str,
    schema: list[bigquery.SchemaField],
    key_column: str,
) -> None:
    """Etapa 3: Carga. Sobe o DataFrame pra staging e faz MERGE no destino.

    Args:
        client: Cliente do BigQuery já autenticado.
        df: DataFrame gerado por `transform`/`transform_events`.
        table_name: Nome-base da tabela (sem o dataset), ex:
            "raw_api_kommo_leads_quiz" ou "raw_api_kommo_events".
        schema: Schema explícito das colunas (LEAD_SCHEMA ou EVENT_SCHEMA).
        key_column: Coluna usada para casar staging x destino no MERGE
            (ex: "lead_id" para leads, "event_id" para eventos).
    """
    logger.info("--- 3. CARGA ---")

    if df.empty:
        logger.info("Nenhuma linha nova/atualizada. Pulando carga.")
        return

    target_table = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}"
    staging_table = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{table_name}_stg"

    job_config = bigquery.LoadJobConfig(
        schema=schema, write_disposition="WRITE_TRUNCATE"
    )
    load_job = client.load_table_from_dataframe(df, staging_table, job_config=job_config)
    load_job.result()

    try:
        client.get_table(target_table)
    except NotFound:
        client.query(
            f"CREATE TABLE `{target_table}` AS SELECT * FROM `{staging_table}` WHERE FALSE"
        ).result()

    update_columns = [field.name for field in schema if field.name != key_column]
    update_clause = ",\n            ".join(
        f"{col} = staging.{col}" for col in update_columns
    )

    merge_query = f"""
        MERGE `{target_table}` AS target
        USING `{staging_table}` AS staging
        ON target.{key_column} = staging.{key_column}
        WHEN MATCHED THEN UPDATE SET
            {update_clause}
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

    checkpoint = get_checkpoint(client, target_table, "updated_at")
    leads = extract(pipeline_id, checkpoint)
    df = transform(leads)
    load(client, df, table_name, LEAD_SCHEMA, "lead_id")


def process_events(client: bigquery.Client) -> None:
    """Orquestra extract/transform/load dos eventos de leads (todos pipelines).

    Args:
        client: Cliente do BigQuery já autenticado.
    """
    target_table = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{EVENTS_TABLE}"
    logger.info(f"=== Eventos de leads -> {target_table} ===")

    checkpoint = get_checkpoint(client, target_table, "created_at")
    events = extract_events(checkpoint)
    df = transform_events(events)
    load(client, df, EVENTS_TABLE, EVENT_SCHEMA, "event_id")


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

    try:
        process_events(client)
    except Exception as err:  # noqa: BLE001 (não deve derrubar o job todo)
        logger.error(f"Falha ao processar eventos: {err}")
        failures.append("events")

    if failures:
        raise RuntimeError(f"Job concluído com falhas nos pipelines: {failures}")

    logger.info("Job finalizado com sucesso! Encerrando container.")


if __name__ == "__main__":
    main()
