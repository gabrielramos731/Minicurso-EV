import asyncio
import asyncpg
from aiomqtt import Client
from datetime import datetime
import transform
from logger import get_logger

log = get_logger(__name__)

async def main():
    miniBatch = []

    async with Client("56.126.34.58") as client:
        await client.subscribe('curso/smartaqua/telemetry/+')
        async for message in client.messages:

            # VERIFICAÇÃO DE INTEGRIDADE
            try:
                leitura = transform.parse_payload(message.payload)
                log.info("Mensagem processada com sucesso | satellite_id=%s", leitura['satellite_id'])
            except (ValueError, TypeError, KeyError) as e:
                log.error("Mensagem descartada | topic=%s | erro=%s", message.topic, e)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScript finalizado.")
