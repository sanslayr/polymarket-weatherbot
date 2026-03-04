from __future__ import annotations


def render_look_help() -> str:
    return (
        "📘 /look 用法\n"
        "- /look <城市或机场代码>\n"
        "- 示例：/look ank | /look London | /look par\n"
        "\n支持城市（示例）：ank / lon / par / nyc / chi / dal / mia / atl / sea / tor / sel / ba / wel\n"
        "提示：统一单条最终报告输出，不发送预告消息。"
    )


def parse_telegram_command(text: str) -> dict[str, str]:
    text = text.strip()
    if not text:
        raise ValueError("Empty command")

    parts = text.split()
    cmd = parts[0].lstrip("/").lower()
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

        params.setdefault("station", tok)
        i += 1

    if "city" in params and "station" not in params:
        params["station"] = params["city"]
    if "icao" in params and "station" not in params:
        params["station"] = params["icao"]
    return params
