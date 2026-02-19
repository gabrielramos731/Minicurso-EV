import asyncio
import json
import asyncpg
import os
from aiomqtt import Client
from datetime import datetime

PG_USER = os.getenv('POSTGRES_USER', 'postgres')
PG_PASS = os.getenv('POSTGRES_PASSWORD', 'senha')
PG_HOST = os.getenv('POSTGRES_HOST', 'timescaledb') # Nome do serviço no Docker
PG_DB   = os.getenv('POSTGRES_DB', 'postgres')
MQTT_HOST = os.getenv('MQTT_BROKER_HOST', 'mosquitto')

DB_DSN = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:5432/{PG_DB}"

async def postMiniBatch(pool, batch):
    try:
        async with pool.acquire() as conn:
            # O copy_records_to_table é ultra rápido para inserir listas
            await conn.copy_records_to_table(
                'sensor_data',
                records=batch,
                columns=['ts', 'temp_ar', 'temp_agua', 'umid', 'id']
            )
            print(f"Lote de {len(batch)} registros salvo no Banco!")
    except Exception as e:
        print(f"Erro ao salvar no banco: {e}")

async def main():
    # Cria o pool de conexões com o banco antes de entrar no loop
    pool = await asyncpg.create_pool(DB_DSN)

    # Lista que vai acumular as TUPLAS para o banco
    miniBatch = []

    try:
        async with Client(MQTT_HOST) as client:
            await client.subscribe("tanque/+/telemetria")
            print("Monitorando todos os clientes (tanque/+/telemetria)...")

            async for message in client.messages:
                try:
                    # 1. Decodifica o JSON
                    payload = message.payload.decode()
                    dados = json.loads(payload)
                    
                    # 2. Prepara os dados (Conversão de Tipos) 
                    timestamp = datetime.strptime(dados["ts"], "%Y-%m-%d %H:%M:%S")
                    esp_id = str(dados["id"])
                    temp_agua = round(float(dados["temp_agua"]), 1)
                    temp_ar = float(dados["temp_ar"])
                    umid = float(dados["umid"])

                    # 3. Cria a TUPLA na ordem exata das colunas do banco
                    registro_banco = (timestamp, temp_ar, temp_agua, umid, esp_id)

                    # Adiciona ao lote
                    miniBatch.append(registro_banco)
                    
                    print(f"Buffer: {len(miniBatch)}/5 - Recebido: {registro_banco}")

                    # 4. Se encheu o lote, manda pro banco
                    if len(miniBatch) >= 5:
                        # O await aqui garante que salvou antes de limpar a lista
                        await postMiniBatch(pool, miniBatch)
                        miniBatch.clear()

                except ValueError as ve:
                    print(f"Erro de conversão de dados: {ve}")
                except Exception as e:
                    print(f"Erro genérico no loop: {e}")

    finally:
        # Fecha a conexão com o banco se o script parar
        await pool.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScript finalizado.")
