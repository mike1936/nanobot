"""WeChat (Weixin) channel implementation using Node.js bridge.

The bridge uses `weixin-agent-sdk` (https://github.com/wong2/weixin-agent-sdk)
to connect to WeChat via long-polling. Communication between the Python backend
and the Node.js bridge is via a local WebSocket connection.

Setup:
1. Install Node.js >= 22 and run ``npm install`` in ``weixin-bridge/``.
2. Run ``npm run login`` once to authenticate with WeChat (scan QR code).
3. Start the bridge: ``npm start`` (defaults to ws://127.0.0.1:3002).
4. Enable this channel in ``~/.nanobot/config.json``.

Because weixin-agent-sdk uses a request-response pattern (each incoming message
expects exactly one reply), every inbound WeChat message has a corresponding
pending request tracked by a unique ID. When the agent produces an outbound
message for a given conversation, the channel pairs it with the pending request
and sends the response back to the bridge.
"""

from __future__ import annotations

import asyncio
import json
import mimetypes
from collections import deque
from typing import Any

from loguru import logger
from pydantic import Field

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import Base


class WeixinConfig(Base):
    """WeChat (Weixin) channel configuration."""

    enabled: bool = False
    bridge_url: str = "ws://127.0.0.1:3002"
    bridge_token: str = ""
    allow_from: list[str] = Field(default_factory=list)


class WeixinChannel(BaseChannel):
    """
    WeChat channel that connects to the Node.js weixin-bridge.

    The bridge uses weixin-agent-sdk's long-polling mechanism to receive
    messages from WeChat without requiring a public server or webhook.
    Communication between Python and Node.js is via a local WebSocket.
    """

    name = "weixin"
    display_name = "WeChat (Weixin)"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return WeixinConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        if isinstance(config, dict):
            config = WeixinConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: WeixinConfig = config
        self._ws = None
        self._connected = False
        # Pending WeChat request IDs, keyed by conversation_id (one queue per user).
        # weixin-agent-sdk is sequential per user, so messages for the same user
        # are processed in order and each needs exactly one reply.
        self._pending: dict[str, deque[str]] = {}

    async def start(self) -> None:
        """Start the WeChat channel by connecting to the bridge."""
        import websockets

        bridge_url = self.config.bridge_url
        logger.info("Connecting to WeChat bridge at {}...", bridge_url)

        self._running = True

        while self._running:
            try:
                async with websockets.connect(bridge_url) as ws:
                    self._ws = ws

                    # Send auth token if configured
                    if self.config.bridge_token:
                        await ws.send(
                            json.dumps({"type": "auth", "token": self.config.bridge_token})
                        )

                    self._connected = True
                    logger.info("Connected to WeChat bridge")

                    async for raw in ws:
                        try:
                            await self._handle_bridge_message(str(raw))
                        except Exception as e:
                            logger.error("Error handling WeChat bridge message: {}", e)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected = False
                self._ws = None
                logger.warning("WeChat bridge connection error: {}", e)

                if self._running:
                    logger.info("Reconnecting to WeChat bridge in 5 seconds...")
                    await asyncio.sleep(5)

    async def stop(self) -> None:
        """Stop the WeChat channel."""
        self._running = False
        self._connected = False

        if self._ws:
            await self._ws.close()
            self._ws = None

    async def send(self, msg: OutboundMessage) -> None:
        """Send a response back to WeChat via the bridge."""
        if not self._ws or not self._connected:
            logger.warning("WeChat bridge not connected — cannot send message to {}", msg.chat_id)
            return

        # Look up the pending request ID for this conversation
        queue = self._pending.get(msg.chat_id)
        if not queue:
            logger.warning(
                "WeChat: no pending request for chat_id '{}' — "
                "cannot deliver outbound message (unsolicited messages are not supported)",
                msg.chat_id,
            )
            return

        request_id = queue.popleft()
        if not queue:
            del self._pending[msg.chat_id]

        payload: dict[str, Any] = {"type": "response", "id": request_id}

        if msg.content:
            payload["text"] = msg.content

        # Attach first media file if present
        if msg.media:
            media_path = msg.media[0]
            mime, _ = mimetypes.guess_type(media_path)
            if mime:
                main_type = mime.split("/")[0]
                if main_type == "image":
                    media_type = "image"
                elif main_type == "video":
                    media_type = "video"
                else:
                    media_type = "file"
            else:
                media_type = "file"

            payload["media"] = {"type": media_type, "url": media_path}

        try:
            await self._ws.send(json.dumps(payload, ensure_ascii=False))
        except Exception as e:
            logger.error("Error sending WeChat message: {}", e)

    async def _handle_bridge_message(self, raw: str) -> None:
        """Handle a message received from the bridge."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Invalid JSON from WeChat bridge: {}", raw[:100])
            return

        msg_type = data.get("type")

        if msg_type == "message":
            request_id = data.get("id", "")
            conversation_id = data.get("conversationId", "")
            text = data.get("text", "")
            media_info = data.get("media")

            if not conversation_id:
                logger.warning("WeChat bridge sent message without conversationId")
                return

            # Queue the pending request ID so send() can pair the reply
            if conversation_id not in self._pending:
                self._pending[conversation_id] = deque()
            self._pending[conversation_id].append(request_id)

            # Build media list if a media attachment was provided
            media: list[str] = []
            if media_info and media_info.get("filePath"):
                media.append(media_info["filePath"])

            # Append a descriptive tag to the text when only media is present
            if media and not text:
                media_type = media_info.get("type", "file")  # type: ignore[union-attr]
                text = f"[{media_type}]"

            await self._handle_message(
                sender_id=conversation_id,
                chat_id=conversation_id,
                content=text,
                media=media,
                metadata={
                    "request_id": request_id,
                    "media_info": media_info,
                },
            )

        elif msg_type == "status":
            status = data.get("status")
            logger.info("WeChat bridge status: {}", status)
            if status == "ready":
                logger.info("WeChat bridge is ready to receive messages")

        elif msg_type == "error":
            logger.error("WeChat bridge error: {}", data.get("error"))
