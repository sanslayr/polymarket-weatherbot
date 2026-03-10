from __future__ import annotations

from station_catalog import common_alias_examples, supported_station_labels


def render_look_help() -> str:
    alias_text = " / ".join(common_alias_examples())
    station_text = " / ".join(supported_station_labels())
    return (
        "📘 /look 用法\n"
        "- /look <城市或机场代码> [YYYY-MM-DD 或 YYYYMMDD]\n"
        "- 示例：/look ank | /look London | /look par 20260307\n"
        f"- 常用别名：{alias_text}\n"
        f"- 支持站点：{station_text}"
    )


def parse_telegram_command(text: str) -> dict[str, str]:
    text = text.strip()
    if not text:
        raise ValueError("Empty command")

    parts = text.split()
    cmd = parts[0].lstrip("/").split("@", 1)[0].lower()
    params: dict[str, str] = {"cmd": cmd}

    key_aliases = {
        "city": "city",
        "icao": "icao",
        "station": "station",
        "model": "model",
        "date": "date",
        "m": "model",
        "d": "date",
        "模型": "model",
        "日期": "date",
    }
    skip_tokens = {"modify", "mod", "update", "set", "修改"}

    def _looks_like_date_token(value: str) -> bool:
        token = str(value or "").strip()
        if len(token) == 8 and token.isdigit():
            return True
        if len(token) == 10 and token[4] == "-" and token[7] == "-":
            y, m, d = token[0:4], token[5:7], token[8:10]
            return y.isdigit() and m.isdigit() and d.isdigit()
        return False

    i = 1
    while i < len(parts):
        tok = parts[i].strip()
        if not tok:
            i += 1
            continue

        lower_tok = tok.lower()
        if lower_tok in skip_tokens:
            i += 1
            continue

        if "=" in tok:
            k, v = tok.split("=", 1)
            k_norm = key_aliases.get(k.strip().lower(), k.strip().lower())
            params[k_norm] = v.strip()
            i += 1
            continue

        if ":" in tok:
            k, v = tok.split(":", 1)
            k_norm = key_aliases.get(k.strip().lower(), k.strip().lower())
            params[k_norm] = v.strip()
            i += 1
            continue

        key_norm = key_aliases.get(lower_tok)
        if key_norm and i + 1 < len(parts):
            params[key_norm] = parts[i + 1].strip()
            i += 2
            continue

        if _looks_like_date_token(tok):
            params.setdefault("date", tok)
            i += 1
            continue

        params.setdefault("station", tok)
        i += 1

    if "city" in params and "station" not in params:
        params["station"] = params["city"]
    if "icao" in params and "station" not in params:
        params["station"] = params["icao"]
    return params
