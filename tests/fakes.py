from typing import Any


class FakeSubscriberBroker:
    def __init__(self) -> None:
        self.handler: Any = None

    def subscriber(self, *args: Any, **kwargs: Any) -> Any:
        def register(handler: Any) -> Any:
            self.handler = handler
            return handler

        return register


class FakePublisherBroker:
    def __init__(self) -> None:
        self.messages: list[tuple[str, Any, bytes, bool]] = []

    async def publish(
        self,
        message: Any,
        *,
        topic: str,
        key: bytes,
        no_confirm: bool,
    ) -> object:
        self.messages.append((topic, message, key, no_confirm))
        return object()
