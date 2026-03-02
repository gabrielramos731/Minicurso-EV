import asyncpg
import os
from dotenv import load_dotenv
from logger import get_logger

load_dotenv()
log = get_logger(__name__)

PG_USER = os.getenv('POSTGRES_USER', 'admin')
PG_PASS = os.getenv('POSTGRES_PASSWORD', 'admin')
PG_HOST = os.getenv('POSTGRES_HOST', 'timescaledb')
PG_PORT = os.getenv('POSTGRES_PORT', 5432)
PG_DB   = os.getenv('POSTGRES_DB', 'minicurso_ev')

DB_DSN = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

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
            #TODO: Inserção em mini batch em schema flexível com JSONB
            

            log.info("Lote de %d registros salvo no banco.", len(records))
    except Exception as e:
        log.error("Erro ao salvar no banco: %s", e)
