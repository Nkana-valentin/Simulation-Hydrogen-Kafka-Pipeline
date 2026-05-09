"""
Single place that loads TSDB.yml and derives the column schema.
Both the consumer and sync service import from here.
"""
import os
from typing import Any, Dict, List, Union

import yaml


def load_schema(config_path: str = "TSDB.yml") -> Dict[str, Any]:
    """
    Load TSDB.yml and return a normalised schema dict:
    {
        "tags":        List[str],
        "fields":      List[str],
        "tag_types":   Dict[str, str],
        "field_types": Dict[str, str],
        "table_name":  str,
    }
    """
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        config = {}
    except yaml.YAMLError as exc:
        raise ValueError(f"Malformed TSDB config at {config_path!r}: {exc}") from exc
    except OSError as exc:
        raise ValueError(f"Cannot read TSDB config at {config_path!r}: {exc}") from exc

    tsdb = config.get("tsdb_schema", {})

    tags_cfg = tsdb.get("tags", [])
    prev_state: Dict[str, float] = tsdb.get("prev_state", {})
    fields_cfg = list(prev_state.keys()) if prev_state else []

    tags = _extract_names(tags_cfg)
    fields = _extract_names(fields_cfg)

    tag_types = _extract_types(tags_cfg, "STRING")
    field_types: Dict[str, str] = {f: "FLOAT" for f in fields}

    table_cfg = tsdb.get("table", {})
    table_name = os.getenv(
        "KAFKA_TOPIC_RAW",
        table_cfg.get("name", "raw_h2_data"),
    )

    return {
        "tags": tags,
        "fields": fields,
        "tag_types": tag_types,
        "field_types": field_types,
        "table_name": table_name,
    }


def _extract_names(items: List[Union[str, Dict]]) -> List[str]:
    names = []
    for item in items or []:
        if isinstance(item, str):
            names.append(item)
        elif isinstance(item, dict) and item.get("name"):
            names.append(str(item["name"]))
    return names


def _extract_types(items: List[Union[str, Dict]], default: str) -> Dict[str, str]:
    types: Dict[str, str] = {}
    for item in items or []:
        if isinstance(item, str):
            types[item] = default
        elif isinstance(item, dict) and item.get("name"):
            types[str(item["name"])] = str(item.get("type", default)).upper()
    return types
