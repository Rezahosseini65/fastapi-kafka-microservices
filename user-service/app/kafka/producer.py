from aiokafka import AIOKafkaProducer


class KafkaProducer:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
        )

        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def send(
        self,
        topic: str,
        value: bytes,
    ) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer is not started")

        await self._producer.send_and_wait(
            topic=topic,
            value=value,
        )