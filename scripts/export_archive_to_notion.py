#!/usr/bin/env python3
"""Export archived report JSON to Notion as a readable report.

Supports:
- create a new page in a Notion database
- append report blocks to an existing page
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"
MAX_RICH_TEXT = 1900
MAX_BLOCKS_PER_REQUEST = 100


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def chunk_text(text: str, size: int = MAX_RICH_TEXT) -> list[str]:
    if not text:
        return [""]
    out = []
    i = 0
    while i < len(text):
        out.append(text[i : i + size])
        i += size
    return out


def paragraph_block(text: str) -> dict[str, Any]:
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [{"type": "text", "text": {"content": text}}]
        },
    }


def heading_block(level: int, text: str) -> dict[str, Any]:
    key = f"heading_{level}"
    return {
        "object": "block",
        "type": key,
        key: {
            "rich_text": [{"type": "text", "text": {"content": text}}]
        },
    }


def bullet_block(text: str) -> dict[str, Any]:
    return {
        "object": "block",
        "type": "bulleted_list_item",
        "bulleted_list_item": {
            "rich_text": [{"type": "text", "text": {"content": text}}]
        },
    }


def report_blocks(doc: dict[str, Any]) -> list[dict[str, Any]]:
    meta = doc.get("meta", {})
    payload = doc.get("payload", {})

    blocks: list[dict[str, Any]] = []
    title = (
        f"{meta.get('station','UNKNOWN')} {meta.get('target_date','')} "
        f"{meta.get('module','report').upper()}"
    ).strip()

    blocks.append(heading_block(1, title))
    blocks.append(paragraph_block(f"Generated at: {utcnow_iso()}"))

    blocks.append(heading_block(2, "Metadata"))
    for line in [
        f"station: {meta.get('station')}",
        f"target_date: {meta.get('target_date')}",
        f"model: {meta.get('model')}",
        f"module: {meta.get('module')}",
        f"updated_at_utc: {meta.get('updated_at_utc')}",
        f"expires_at_utc: {meta.get('expires_at_utc')}",
        f"source: {meta.get('source')}",
    ]:
        blocks.append(bullet_block(line))

    if isinstance(payload, dict) and payload.get("summary"):
        blocks.append(heading_block(2, "Summary"))
        for part in chunk_text(str(payload.get("summary"))):
            blocks.append(paragraph_block(part))

    sections = payload.get("sections") if isinstance(payload, dict) else None
    if isinstance(sections, dict) and sections:
        blocks.append(heading_block(2, "Sections"))
        for k, v in sections.items():
            blocks.append(heading_block(3, str(k)))
            for part in chunk_text(str(v)):
                blocks.append(paragraph_block(part))

    blocks.append(heading_block(2, "Raw Payload"))
    raw = json.dumps(payload, ensure_ascii=False, indent=2)
    for part in chunk_text(raw):
        blocks.append(paragraph_block(part))

    return blocks


def notion_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def create_page_in_database(
    token: str,
    database_id: str,
    title: str,
    blocks: list[dict[str, Any]],
    title_prop: str,
) -> dict[str, Any]:
    url = f"{NOTION_API_BASE}/pages"
    body = {
        "parent": {"database_id": database_id},
        "properties": {
            title_prop: {
                "title": [{"type": "text", "text": {"content": title[:120]}}]
            }
        },
        "children": blocks,
    }
    r = requests.post(url, headers=notion_headers(token), json=body, timeout=45)
    r.raise_for_status()
    return r.json()


def append_blocks_to_page(token: str, page_id: str, blocks: list[dict[str, Any]]) -> dict[str, Any]:
    url = f"{NOTION_API_BASE}/blocks/{page_id}/children"
    body = {"children": blocks}
    r = requests.patch(url, headers=notion_headers(token), json=body, timeout=45)
    r.raise_for_status()
    return r.json()


def chunk_blocks(blocks: list[dict[str, Any]], size: int = MAX_BLOCKS_PER_REQUEST) -> list[list[dict[str, Any]]]:
    return [blocks[i : i + size] for i in range(0, len(blocks), size)]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Export archive JSON report to Notion")
    p.add_argument("--archive-file", required=True)
    p.add_argument("--notion-token", default=os.getenv("NOTION_TOKEN", ""))
    p.add_argument("--database-id", default="")
    p.add_argument("--page-id", default="")
    p.add_argument("--title-prop", default="Name")
    p.add_argument("--title", default="")
    p.add_argument("--dry-run", action="store_true")
    return p


def main() -> None:
    args = build_parser().parse_args()
    if not args.database_id and not args.page_id:
        raise SystemExit("Either --database-id or --page-id is required")

    doc = load_json(Path(args.archive_file))
    meta = doc.get("meta", {})
    auto_title = (
        f"{meta.get('station','UNKNOWN')} {meta.get('target_date','')} "
        f"{meta.get('module','report').upper()}"
    ).strip()
    title = args.title or auto_title

    blocks = report_blocks(doc)

    if args.dry_run:
        print(json.dumps({"title": title, "blocks": len(blocks)}, ensure_ascii=True, indent=2))
        return

    token = args.notion_token.strip()
    if not token:
        raise SystemExit("Missing Notion token: pass --notion-token or set NOTION_TOKEN")

    if args.database_id:
        chunks = chunk_blocks(blocks)
        first = chunks[0]
        res = create_page_in_database(
            token=token,
            database_id=args.database_id,
            title=title,
            blocks=first,
            title_prop=args.title_prop,
        )
        page_id = res.get("id")
        for extra in chunks[1:]:
            append_blocks_to_page(token=token, page_id=page_id, blocks=extra)
        print(
            json.dumps(
                {
                    "status": "created",
                    "id": page_id,
                    "url": res.get("url"),
                    "chunks": len(chunks),
                    "blocks": len(blocks),
                },
                ensure_ascii=True,
                indent=2,
            )
        )
        return

    chunks = chunk_blocks(blocks)
    result_count = 0
    for part in chunks:
        res = append_blocks_to_page(token=token, page_id=args.page_id, blocks=part)
        result_count += len(res.get("results", []))
    print(
        json.dumps(
            {
                "status": "appended",
                "page_id": args.page_id,
                "result_count": result_count,
                "chunks": len(chunks),
                "blocks": len(blocks),
            },
            ensure_ascii=True,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
