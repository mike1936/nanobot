"""Tests for the WeChat (Weixin) channel adapter."""

from __future__ import annotations

import asyncio
import json
from collections import deque
from unittest.mock import AsyncMock, MagicMock

import pytest

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.weixin import WeixinChannel, WeixinConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_channel(allow_from: list[str] | None = None) -> WeixinChannel:
    cfg = WeixinConfig(
        enabled=True,
        bridge_url="ws://127.0.0.1:3002",
        bridge_token="",
        allow_from=allow_from or ["*"],
    )
    return WeixinChannel(cfg, MessageBus())


def _fake_ws() -> MagicMock:
    ws = MagicMock()
    ws.send = AsyncMock()
    ws.close = AsyncMock()
    return ws


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def test_default_config_structure() -> None:
    cfg = WeixinChannel.default_config()
    assert cfg["enabled"] is False
    assert "bridgeUrl" in cfg
    assert "allowFrom" in cfg


def test_config_from_dict() -> None:
    ch = WeixinChannel(
        {"enabled": True, "bridgeUrl": "ws://127.0.0.1:3002", "allowFrom": ["*"]},
        MessageBus(),
    )
    assert ch.config.bridge_url == "ws://127.0.0.1:3002"
    assert ch.config.allow_from == ["*"]


def test_config_bridge_token() -> None:
    ch = WeixinChannel(
        {"enabled": True, "bridgeToken": "mysecret", "allowFrom": ["*"]},
        MessageBus(),
    )
    assert ch.config.bridge_token == "mysecret"


# ---------------------------------------------------------------------------
# _handle_bridge_message — incoming WeChat messages
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_bridge_message_queues_pending_request() -> None:
    ch = _make_channel()
    raw = json.dumps(
        {
            "type": "message",
            "id": "req-001",
            "conversationId": "user123",
            "text": "hello",
        }
    )
    await ch._handle_bridge_message(raw)

    assert "user123" in ch._pending
    assert ch._pending["user123"][0] == "req-001"


@pytest.mark.asyncio
async def test_handle_bridge_message_publishes_inbound() -> None:
    ch = _make_channel()
    raw = json.dumps(
        {
            "type": "message",
            "id": "req-001",
            "conversationId": "user123",
            "text": "hello",
        }
    )
    await ch._handle_bridge_message(raw)

    msg = await ch.bus.consume_inbound()
    assert msg.channel == "weixin"
    assert msg.sender_id == "user123"
    assert msg.chat_id == "user123"
    assert msg.content == "hello"


@pytest.mark.asyncio
async def test_handle_bridge_message_with_media() -> None:
    ch = _make_channel()
    raw = json.dumps(
        {
            "type": "message",
            "id": "req-002",
            "conversationId": "user456",
            "text": "",
            "media": {
                "type": "image",
                "filePath": "/tmp/photo.jpg",
                "mimeType": "image/jpeg",
            },
        }
    )
    await ch._handle_bridge_message(raw)

    msg = await ch.bus.consume_inbound()
    assert msg.media == ["/tmp/photo.jpg"]
    assert msg.content == "[image]"


@pytest.mark.asyncio
async def test_handle_bridge_message_access_denied() -> None:
    ch = _make_channel(allow_from=["allowed_user"])
    raw = json.dumps(
        {
            "type": "message",
            "id": "req-003",
            "conversationId": "blocked_user",
            "text": "hi",
        }
    )
    await ch._handle_bridge_message(raw)

    # Message should NOT be published since sender is not allowed
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(ch.bus.consume_inbound(), timeout=0.05)


@pytest.mark.asyncio
async def test_handle_bridge_message_invalid_json() -> None:
    ch = _make_channel()
    # Should not raise
    await ch._handle_bridge_message("not json at all")


@pytest.mark.asyncio
async def test_handle_bridge_message_status_ready() -> None:
    ch = _make_channel()
    raw = json.dumps({"type": "status", "status": "ready"})
    # Should not raise
    await ch._handle_bridge_message(raw)


@pytest.mark.asyncio
async def test_handle_bridge_message_no_conversation_id() -> None:
    ch = _make_channel()
    raw = json.dumps({"type": "message", "id": "req-x", "text": "oops"})
    # Should warn and skip — no pending entry created
    await ch._handle_bridge_message(raw)
    assert not ch._pending


# ---------------------------------------------------------------------------
# send — outbound messages paired with pending requests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_pairs_pending_request() -> None:
    ch = _make_channel()
    ch._ws = _fake_ws()
    ch._connected = True

    # Simulate a pending request
    ch._pending["user123"] = deque(["req-001"])

    await ch.send(OutboundMessage(channel="weixin", chat_id="user123", content="hello back"))

    ch._ws.send.assert_called_once()
    payload = json.loads(ch._ws.send.call_args[0][0])
    assert payload["type"] == "response"
    assert payload["id"] == "req-001"
    assert payload["text"] == "hello back"


@pytest.mark.asyncio
async def test_send_removes_consumed_request_id() -> None:
    ch = _make_channel()
    ch._ws = _fake_ws()
    ch._connected = True

    ch._pending["user123"] = deque(["req-001"])
    await ch.send(OutboundMessage(channel="weixin", chat_id="user123", content="hi"))

    # Pending queue for user123 should be gone after consuming the only ID
    assert "user123" not in ch._pending


@pytest.mark.asyncio
async def test_send_multiple_requests_in_order() -> None:
    ch = _make_channel()
    ch._ws = _fake_ws()
    ch._connected = True

    ch._pending["user123"] = deque(["req-001", "req-002"])

    await ch.send(OutboundMessage(channel="weixin", chat_id="user123", content="first"))
    payload1 = json.loads(ch._ws.send.call_args_list[0][0][0])
    assert payload1["id"] == "req-001"

    await ch.send(OutboundMessage(channel="weixin", chat_id="user123", content="second"))
    payload2 = json.loads(ch._ws.send.call_args_list[1][0][0])
    assert payload2["id"] == "req-002"


@pytest.mark.asyncio
async def test_send_no_pending_request_logs_warning() -> None:
    ch = _make_channel()
    ch._ws = _fake_ws()
    ch._connected = True

    # No pending request registered for this chat_id
    await ch.send(OutboundMessage(channel="weixin", chat_id="unknown_user", content="hello"))

    ch._ws.send.assert_not_called()


@pytest.mark.asyncio
async def test_send_not_connected_logs_warning() -> None:
    ch = _make_channel()
    ch._ws = None
    ch._connected = False

    ch._pending["user123"] = deque(["req-001"])

    await ch.send(OutboundMessage(channel="weixin", chat_id="user123", content="hi"))

    # pending should remain untouched (we returned early)
    assert "user123" in ch._pending


@pytest.mark.asyncio
async def test_send_with_media_image() -> None:
    ch = _make_channel()
    ch._ws = _fake_ws()
    ch._connected = True

    ch._pending["user123"] = deque(["req-001"])

    await ch.send(
        OutboundMessage(
            channel="weixin",
            chat_id="user123",
            content="here is a photo",
            media=["/tmp/photo.jpg"],
        )
    )

    payload = json.loads(ch._ws.send.call_args[0][0])
    assert payload["media"]["type"] == "image"
    assert payload["media"]["url"] == "/tmp/photo.jpg"


@pytest.mark.asyncio
async def test_send_with_media_unknown_type() -> None:
    ch = _make_channel()
    ch._ws = _fake_ws()
    ch._connected = True

    ch._pending["user123"] = deque(["req-001"])

    await ch.send(
        OutboundMessage(
            channel="weixin",
            chat_id="user123",
            content="",
            media=["/tmp/document.xyz"],
        )
    )

    payload = json.loads(ch._ws.send.call_args[0][0])
    assert payload["media"]["type"] == "file"


# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stop_closes_ws() -> None:
    ch = _make_channel()
    ws = _fake_ws()
    ch._ws = ws
    ch._connected = True
    ch._running = True

    await ch.stop()

    assert ch._running is False
    assert ch._connected is False
    ws.close.assert_called_once()
    assert ch._ws is None


# ---------------------------------------------------------------------------
# Full round-trip: bridge message → send response
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_round_trip_message_and_send() -> None:
    ch = _make_channel()
    ch._ws = _fake_ws()
    ch._connected = True

    raw = json.dumps(
        {
            "type": "message",
            "id": "req-trip",
            "conversationId": "user_rt",
            "text": "ping",
        }
    )
    await ch._handle_bridge_message(raw)

    # Consume the inbound message (as the agent would)
    msg = await ch.bus.consume_inbound()
    assert msg.content == "ping"

    # Agent sends reply
    await ch.send(OutboundMessage(channel="weixin", chat_id=msg.chat_id, content="pong"))

    payload = json.loads(ch._ws.send.call_args[0][0])
    assert payload["id"] == "req-trip"
    assert payload["text"] == "pong"
