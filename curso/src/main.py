import asyncio
import transform
import db_loader
from aiomqtt import Client
from logger import get_logger

log = get_logger(__name__)

async def main():
    await db_loader.init_pool()
    mini_batch = []

    try:
        async with Client("56.126.34.58") as client:
            await client.subscribe('curso/smartaqua/telemetry/+')
            async for message in client.messages:

                # VERIFICAÇÃO DE INTEGRIDADE
                try:
                    leitura = transform.parse_payload(message.payload)
                    log.info("Mensagem processada com sucesso | satellite_id=%s", leitura[0][1])
                    mini_batch.append(leitura)

                    # CARGA PARA O BANCO DE DADOS
                    if len(mini_batch) >= 15:
                        await db_loader.insert_minibatch(mini_batch)
                        mini_batch.clear()

                except (ValueError, TypeError, KeyError) as e:
                    log.error("Mensagem descartada | topic=%s | erro=%s", message.topic, e)
    finally:
        await db_loader.close_pool()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScript finalizado.")
