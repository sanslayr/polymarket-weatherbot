#!/usr/bin/env python3
"""Telegram command entrypoint for city Tmax report.

Examples:
  /look Ankara
  /look city=Ankara date=2026-03-03
  /look icao=LTAC model=ecmwf
  /look city=Toronto model=gfs
  /look Ankara modify model gfs date 2026-03-04
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


def _reexec_into_skill_venv() -> None:
    if str(os.getenv("WEATHERBOT_SKIP_VENV_REEXEC", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}:
        return
    script_path = Path(__file__).resolve()
    venv_python = script_path.parent.parent / ".venv_gfs" / "bin" / "python"
    if not venv_python.exists():
        return
    current_python = Path(sys.executable).resolve() if sys.executable else None
    try:
        target_python = venv_python.resolve()
    except FileNotFoundError:
        return
    if current_python == target_python:
        return
    env = dict(os.environ)
    env["WEATHERBOT_SKIP_VENV_REEXEC"] = "1"
    os.execvpe(str(target_python), [str(target_python), str(script_path), *sys.argv[1:]], env)


_reexec_into_skill_venv()

PROCESS_T0 = time.perf_counter()
TRACE_LOG_PATH = Path(__file__).resolve().parents[4] / "logs" / "look-cli-latency.jsonl"

def _format_local_header_time(now_utc: datetime, now_local: datetime) -> str:
    local_time = now_local.strftime("%H:%M")
    if now_local.date() != now_utc.date():
        local_time = f"{now_local.month}/{now_local.day} {local_time}"
    return f"{local_time} Local ({format_utc_offset(now_local)})"

from look_change_guard import build_unchanged_notice
from look_command import parse_telegram_command, render_look_help
from look_runtime_control import LookRuntimeContext, LookRuntimeController, build_request_key
from station_catalog import (
    Station,
    default_model_for_station,
    format_utc_offset,
    resolve_station,
    site_tag_for as _site_tag_for,
    station_timezone_name,
    terrain_tag_for as _terrain_tag_for,
)
def _resolve_target_date_for_station(raw_target_date: str | None, tz_name_station: str) -> str:
    normalized = raw_target_date
    if normalized and len(normalized) == 8 and normalized.isdigit():
        normalized = f"{normalized[0:4]}-{normalized[4:6]}-{normalized[6:8]}"
    now_utc = datetime.now(timezone.utc)
    if normalized:
        return normalized
    try:
        return now_utc.astimezone(ZoneInfo(tz_name_station)).strftime("%Y-%m-%d")
    except Exception:
        return now_utc.strftime("%Y-%m-%d")
def _perf_log(stage: str, seconds: float) -> None:
    _ = (stage, seconds)
    return


def _emit_runtime_trace(payload: dict[str, object]) -> None:
    try:
        TRACE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            **payload,
        }
        with TRACE_LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _render_report_header(st: Station, bundle: LookReportBundle) -> str:
    lat_hemi = "N" if st.lat >= 0 else "S"
    lon_hemi = "E" if st.lon >= 0 else "W"
    terrain_tag = _terrain_tag_for(st.icao)
    site_tag = _site_tag_for(st.icao)
    head_geo = f"{abs(st.lat):.4f}{lat_hemi}, {abs(st.lon):.4f}{lon_hemi}"
    if site_tag:
        head_geo = f"{head_geo} ({site_tag})"
    elif terrain_tag:
        head_geo = f"{head_geo} ({terrain_tag})"

    header_lines = [
        f"📍 **{st.icao} ({st.city}) | {head_geo}**",
        f"生成时间: {bundle.now_utc.strftime('%Y/%m/%d %H:%M')} UTC | {_format_local_header_time(bundle.now_utc, bundle.now_local)}",
    ]

    if bundle.mode == "metar_only":
        header_lines.append("数据提醒: 当前按实况降级生成。")
    else:
        try:
            quality = bundle.forecast_quality
            missing = set(quality.get("missing_layers") or [])
            degraded = str(quality.get("source_state") or "") == "degraded"
            syn_fail = ("synoptic" in missing) or degraded
            provider_requested_3d = str(quality.get("synoptic_provider_requested") or bundle.synoptic_provider_used)
            provider_used_3d = str(quality.get("synoptic_provider_used") or bundle.synoptic_provider_used)
            if (provider_requested_3d and provider_requested_3d != provider_used_3d) or (syn_fail and bundle.synoptic_error):
                header_lines.append("数据提醒: 高空背景存在降级，部分环流诊断可能偏弱。")
        except Exception:
            pass

    header_lines.append("**🦞龙虾学习中，不提供交易建议🦞**")
    return "\n".join(header_lines)


def render_report(
    command_text: str,
    *,
    channel: str | None = None,
    peer_kind: str | None = None,
    peer_id: str | None = None,
    sender_id: str | None = None,
    session_key: str | None = None,
) -> str:
    t_e2e = time.perf_counter()
    bootstrap_elapsed = max(0.0, t_e2e - PROCESS_T0)
    perf_local: dict[str, float] = {}
    trace_base: dict[str, object] = {
        "command": command_text,
        "channel": channel or "",
        "peer_kind": peer_kind or "",
        "peer_id": peer_id or "",
        "sender_id": sender_id or "",
        "session_key": session_key or "",
    }

    def _mark(stage: str, seconds: float) -> None:
        perf_local[stage] = float(seconds)
        _perf_log(stage, seconds)

    params = parse_telegram_command(command_text)
    if params.get("cmd") != "look":
        raise ValueError("Unsupported command. Use /look")

    station_hint = params.get("station")
    if not station_hint:
        return render_look_help()
    if str(station_hint).strip().lower() in {"help", "帮助", "h"}:
        return render_look_help()

    try:
        st = resolve_station(station_hint)
    except ValueError as exc:
        return f"{exc}\n\n{render_look_help()}"
    tz_name_station = station_timezone_name(st)

    target_date = _resolve_target_date_for_station(params.get("date"), tz_name_station)

    try:
        datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("date must be YYYY-MM-DD (or YYYYMMDD)") from exc

    model = default_model_for_station(st).lower()
    if model not in {"gfs", "ecmwf"}:
        model = "ecmwf"
    trace_base["model"] = model

    runtime_context = LookRuntimeContext.from_runtime(
        channel=channel,
        peer_kind=peer_kind,
        peer_id=peer_id,
        sender_id=sender_id,
        session_key=session_key,
    )
    runtime_control = LookRuntimeController(
        context=runtime_context,
        compute_key=build_request_key(station_icao=st.icao, target_date=target_date),
        query_label=f"{st.city}({st.icao})-{target_date.replace('-', '')}",
    )
    cached_payload = runtime_control.peek_cached_result_payload()
    if cached_payload:
        unchanged_notice = build_unchanged_notice(
            query_label=f"{st.city}({st.icao})-{target_date.replace('-', '')}",
            icao=st.icao,
            model=model,
            cached_payload=cached_payload,
        )
        if unchanged_notice:
            if not runtime_control.should_emit_unchanged_notice(cached_payload):
                result = runtime_control.deliver_cached_or_notice(cached_payload, notice=unchanged_notice)
                _emit_runtime_trace(
                    {
                        **trace_base,
                        "station": st.icao,
                        "target_date": target_date,
                        "result_kind": "cached_payload",
                        "elapsed_s": round(time.perf_counter() - t_e2e, 3),
                        "bootstrap_s": round(bootstrap_elapsed, 3),
                    }
                )
                return result
            result = runtime_control.deliver_unchanged_notice(cached_payload, notice=unchanged_notice)
            _emit_runtime_trace(
                {
                    **trace_base,
                    "station": st.icao,
                    "target_date": target_date,
                    "result_kind": "unchanged_notice",
                    "elapsed_s": round(time.perf_counter() - t_e2e, 3),
                    "bootstrap_s": round(bootstrap_elapsed, 3),
                }
            )
            return result
    preflight = runtime_control.preflight()
    if not preflight.proceed:
        result = str(preflight.text or "")
        _emit_runtime_trace(
            {
                **trace_base,
                "station": st.icao,
                "target_date": target_date,
                "result_kind": "preflight_block",
                "elapsed_s": round(time.perf_counter() - t_e2e, 3),
                "bootstrap_s": round(bootstrap_elapsed, 3),
            }
        )
        return result

    # 统一输出：固定走当前主报告链路，对外命令仅暴露 station/date。
    try:
        from hourly_data_service import prune_runtime_cache as prune_runtime_cache
        from look_change_guard import build_cached_result_meta
        from look_report_service import build_look_report_bundle

        prune_runtime_cache()
        bundle = build_look_report_bundle(
            station=st,
            target_date=target_date,
            model=model,
            tz_name_station=tz_name_station,
            perf_log=_mark,
        )
        total_elapsed = time.perf_counter() - t_e2e
        header = _render_report_header(st, bundle)

        show_perf = str(os.getenv("LOOK_SHOW_PERF", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
        if show_perf:
            perf_line = (
                f"⏱️ 模块耗时: total {total_elapsed:.2f}s | bootstrap {bootstrap_elapsed:.2f}s | process {total_elapsed + bootstrap_elapsed:.2f}s"
                f" | hourly {float(perf_local.get('hourly_fetch', 0.0) or 0.0):.2f}s"
                f" | metar {float(perf_local.get('metar_fetch_parse', 0.0) or 0.0):.2f}s"
                f" | forecast {float(perf_local.get('forecast_pipeline', 0.0) or 0.0):.2f}s"
                f" (syn {float(perf_local.get('forecast.synoptic_build', 0.0) or 0.0):.2f}s"
                f", dec {float(perf_local.get('forecast.decision_build', 0.0) or 0.0):.2f}s"
                f", cacheR {float(perf_local.get('forecast.cache_read', 0.0) or 0.0):.2f}s"
                f", cacheW {float(perf_local.get('forecast.cache_write', 0.0) or 0.0):.2f}s)"
                f" | render {float(perf_local.get('render_body', 0.0) or 0.0):.2f}s"
            )
            header = f"{header}\n{perf_line}"

        result_text = "\n\n".join(part.strip() for part in [header, bundle.body] if str(part or "").strip())
        if str(bundle.footer or "").strip():
            result_text = f"{result_text}\n{bundle.footer.strip()}"
        result_text = re.sub(r"\n{3,}", "\n\n", result_text).strip()
        runtime_control.success(
            result_text,
            result_meta=build_cached_result_meta(
                icao=st.icao,
                model=bundle.model,
                metar24=bundle.metar24,
            ),
        )
        _emit_runtime_trace(
            {
                **trace_base,
                "station": st.icao,
                "target_date": target_date,
                "result_kind": "success",
                "elapsed_s": round(total_elapsed, 3),
                "bootstrap_s": round(bootstrap_elapsed, 3),
                "output_chars": len(result_text),
                "perf": {k: round(float(v), 3) for k, v in perf_local.items()},
            }
        )
        return result_text
    except Exception as exc:
        runtime_control.failure()
        _emit_runtime_trace(
            {
                **trace_base,
                "station": st.icao,
                "target_date": target_date,
                "result_kind": "error",
                "elapsed_s": round(time.perf_counter() - t_e2e, 3),
                "bootstrap_s": round(bootstrap_elapsed, 3),
                "error": str(exc),
                "perf": {k: round(float(v), 3) for k, v in perf_local.items()},
            }
        )
        raise


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate report text from Telegram-style command")
    p.add_argument("--command", required=True, help="Telegram command text, e.g. '/look Ankara model=ecmwf'")
    p.add_argument("--channel", help="Optional runtime channel, e.g. telegram")
    p.add_argument("--peer-kind", help="Optional runtime peer kind, e.g. group|direct")
    p.add_argument("--peer-id", help="Optional runtime peer/chat id")
    p.add_argument("--sender-id", help="Optional runtime sender id")
    p.add_argument("--session-key", help="Optional OpenClaw session key")
    return p


def main() -> None:
    args = build_parser().parse_args()
    try:
        print(
            render_report(
                args.command,
                channel=args.channel,
                peer_kind=args.peer_kind,
                peer_id=args.peer_id,
                sender_id=args.sender_id,
                session_key=args.session_key,
            )
        )
    except Exception as exc:
        print(f"❌ /look 执行失败: {exc}")


if __name__ == "__main__":
    main()
