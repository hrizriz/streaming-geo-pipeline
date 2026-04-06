"""Alert ke Discord / Slack / Telegram (webhook)."""

from __future__ import annotations

import logging
import os

import requests

log = logging.getLogger(__name__)

MAX_LEN = 1800


def _truncate(s: str, n: int = MAX_LEN) -> str:
    s = s.strip()
    if len(s) <= n:
        return s
    return s[: n - 3] + "..."


def _discord(title: str, body: str, url: str, timeout: float = 15.0) -> bool:
    content = _truncate(f"**{title}**\n{body}")
    try:
        r = requests.post(url, json={"content": content}, timeout=timeout)
        if r.status_code in (200, 204):
            return True
        log.warning("Discord webhook http %s: %s", r.status_code, r.text[:200])
    except requests.RequestException as e:
        log.warning("Discord webhook error: %s", e)
    return False


def _slack(title: str, body: str, url: str, timeout: float = 15.0) -> bool:
    text = _truncate(f"*{title}*\n{body}")
    try:
        r = requests.post(url, json={"text": text}, timeout=timeout)
        if r.status_code == 200:
            return True
        log.warning("Slack webhook http %s: %s", r.status_code, r.text[:200])
    except requests.RequestException as e:
        log.warning("Slack webhook error: %s", e)
    return False


def _telegram(title: str, body: str, token: str, chat_id: str, timeout: float = 15.0) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    text = _truncate(f"{title}\n{body}", 4000)
    try:
        r = requests.post(
            url,
            data={"chat_id": chat_id, "text": text},
            timeout=timeout,
        )
        data = r.json()
        if r.status_code == 200 and data.get("ok"):
            return True
        log.warning("Telegram sendMessage: %s", data)
    except requests.RequestException as e:
        log.warning("Telegram error: %s", e)
    return False


def send_probe_alerts(
    *,
    title: str,
    body: str,
    discord_url: str | None = None,
    slack_url: str | None = None,
    telegram_token: str | None = None,
    telegram_chat_id: str | None = None,
) -> list[str]:
    ok_channels: list[str] = []

    du = (discord_url or os.environ.get("DISCORD_WEBHOOK_URL", "")).strip()
    su = (slack_url or os.environ.get("SLACK_WEBHOOK_URL", "")).strip()
    tt = (telegram_token or os.environ.get("TELEGRAM_BOT_TOKEN", "")).strip()
    tc = (telegram_chat_id or os.environ.get("TELEGRAM_CHAT_ID", "")).strip()

    if du:
        if _discord(title, body, du):
            ok_channels.append("discord")
    if su:
        if _slack(title, body, su):
            ok_channels.append("slack")
    if tt and tc:
        if _telegram(title, body, tt, tc):
            ok_channels.append("telegram")

    return ok_channels


def any_channel_configured() -> bool:
    return bool(
        os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
        or os.environ.get("SLACK_WEBHOOK_URL", "").strip()
        or (
            os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
            and os.environ.get("TELEGRAM_CHAT_ID", "").strip()
        )
    )
