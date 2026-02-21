import asyncpg
import os
from logger import get_logger

log = get_logger(__name__)

PG_USER = os.getenv('POSTGRES_USER', 'postgres')
PG_PASS = os.getenv('POSTGRES_PASSWORD', 'senha')
PG_HOST = os.getenv('POSTGRES_HOST', 'timescaledb')
PG_DB   = os.getenv('POSTGRES_DB', 'postgres')

DB_DSN = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:5432/{PG_DB}"

pool = None

async def init_pool():
    global pool
    pool = await asyncpg.create_pool(DB_DSN)
    log.info("Pool de conexões criado.")

async def close_pool():
    await pool.close()
    log.info("Pool de conexões encerrado.")

async def insert_minibatch(mini_batch):
    records = [
        (metadados[0], metadados[1], metadados[2], payload)
        for metadados, payload in mini_batch
    ]
    try:
        async with pool.acquire() as conn:
            await conn.copy_records_to_table(
                'satellite_data',
                records=records,
                columns=['ts', 'satellite_id', 'health_status', 'payload']
            )
            log.info("Lote de %d registros salvo no banco.", len(records))
    except Exception as e:
        log.error("Erro ao salvar no banco: %s", e)
