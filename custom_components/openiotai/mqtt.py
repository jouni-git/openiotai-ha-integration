"""MQTT publishing utilities for OpenIOTAI integration."""

from __future__ import annotations

import json
import logging
from typing import Any, Dict

from homeassistant.core import HomeAssistant
from homeassistant.components import mqtt

_LOGGER = logging.getLogger(__name__)

DEFAULT_TOPIC_PREFIX = "data/hamqttexport"


async def publish_snapshot(
    hass: HomeAssistant,
    data: Dict[str, Any],
    topic_prefix: str = DEFAULT_TOPIC_PREFIX,
) -> None:
    """
    Publish a snapshot of entity states to MQTT.

    Each entity is published as a separate MQTT message:
      <topic_prefix>/<entity_id>

    Payload is JSON encoded.
    """
    if not mqtt.is_connected(hass):
        _LOGGER.warning("MQTT is not connected, skipping publish")
        return

    for entity_id, state in data.items():
        topic = f"{topic_prefix}/{entity_id}"

        payload = json.dumps(
            {
                "entity_id": entity_id,
                "state": state.get("state"),
                "attributes": state.get("attributes", {}),
            },
            ensure_ascii=False,
        )

        await mqtt.async_publish(
            hass,
            topic,
            payload,
            qos=0,
            retain=False,
        )

    _LOGGER.debug(
        "Published OpenIOTAI MQTT snapshot (%d entities)",
        len(data),
    )
