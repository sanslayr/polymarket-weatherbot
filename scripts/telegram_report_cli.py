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
import os
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

def _format_local_header_time(now_utc: datetime, now_local: datetime) -> str:
    local_time = now_local.strftime("%H:%M")
    if now_local.date() != now_utc.date():
        local_time = f"{now_local.month}/{now_local.day} {local_time}"
    return f"{local_time} Local ({format_utc_offset(now_local)})"

from hourly_data_service import prune_runtime_cache as _prune_runtime_cache
from look_change_guard import build_cached_result_meta, build_unchanged_notice
from look_command import parse_telegram_command, render_look_help
from look_report_service import LookReportBundle, build_look_report_bundle
from look_runtime_control import LookRuntimeContext, LookRuntimeController, build_request_key
from station_catalog import (
    Station,
    default_model_for_station,
    direction_factor_for as _direction_factor_for,
    format_utc_offset,
    resolve_station,
    site_tag_for as _site_tag_for,
    station_timezone_name,
    terrain_tag_for as _terrain_tag_for,
)
from telegram_notifier import send_telegram_message

LOOK_SEND_PENDING_NOTICE = str(os.getenv("LOOK_SEND_PENDING_NOTICE", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}

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


def _parse_env_int(name: str) -> int | None:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _send_pending_look_notice(st: Station, target_date: str) -> None:
    if not LOOK_SEND_PENDING_NOTICE:
        return
    channel = str(os.getenv("OPENCLAW_CHANNEL") or "").strip().lower()
    if channel != "telegram":
        return
    peer_id = str(os.getenv("OPENCLAW_PEER_ID") or "").strip()
    account_id = str(os.getenv("OPENCLAW_ACCOUNT_ID") or "weatherbot").strip() or "weatherbot"
    reply_to_message_id = _parse_env_int("OPENCLAW_MESSAGE_ID")
    message_thread_id = _parse_env_int("OPENCLAW_THREAD_ID")
    if not peer_id or reply_to_message_id is None:
        return
    query_label = f"{st.city}({st.icao})-{target_date.replace('-', '')}"
    text = f"👀正在查看{query_label}天气形势......"
    try:
        send_telegram_message(
            text,
            chat_id=peer_id,
            account=account_id,
            parse_mode=None,
            disable_web_page_preview=True,
            reply_to_message_id=reply_to_message_id,
            message_thread_id=message_thread_id,
            timeout=5.0,
        )
    except Exception:
        return


def _perf_log(stage: str, seconds: float) -> None:
    _ = (stage, seconds)
    return


def _format_runtime_tag(tag: str) -> str:
    txt = str(tag or "").strip()
    if len(txt) == 10 and txt.isdigit():
        return f"{txt[0:4]}/{txt[4:6]}/{txt[6:8]} {txt[8:10]}Z"
    if txt.endswith("Z") and len(txt) == 11 and txt[:10].isdigit():
        return f"{txt[0:4]}/{txt[4:6]}/{txt[6:8]} {txt[8:10]}Z"
    return txt


def _render_report_header(st: Station, bundle: LookReportBundle) -> str:
    lat_hemi = "N" if st.lat >= 0 else "S"
    lon_hemi = "E" if st.lon >= 0 else "W"
    terrain_tag = _terrain_tag_for(st.icao)
    site_tag = _site_tag_for(st.icao)
    direction_factor = _direction_factor_for(st.icao)
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
        header_lines.append(
            f"分析链路: 小时预报/3D背景不可用（最近运行时次参考: {_format_runtime_tag(bundle.runtime_utc)}）"
        )
    else:
        if not bundle.compact_synoptic:
            syn_rt_fmt = _format_runtime_tag(bundle.synoptic_runtime_used)
            syn_prev_rt_fmt = _format_runtime_tag(bundle.synoptic_previous_runtime_used)
            cycle_bits = []
            if syn_rt_fmt:
                cycle_bits.append(syn_rt_fmt + (f" {bundle.synoptic_stream_used}" if bundle.synoptic_stream_used else ""))
            elif bundle.runtime_utc:
                cycle_bits.append(_format_runtime_tag(bundle.runtime_utc))
            if syn_prev_rt_fmt and syn_prev_rt_fmt != syn_rt_fmt:
                cycle_bits.append(f"对比前一时次 {syn_prev_rt_fmt}")
            cycle_txt = " | ".join(cycle_bits) if cycle_bits else _format_runtime_tag(bundle.runtime_utc)
            header_lines.append(
                f"分析链路: 小时预报源 {bundle.provider_used} | 数值预报场源 {bundle.synoptic_provider_used}（数值模型时次: {cycle_txt}）"
            )
            if direction_factor:
                header_lines.append(f"方位因子: {direction_factor}")

        try:
            quality = bundle.forecast_quality
            missing = set(quality.get("missing_layers") or [])
            degraded = str(quality.get("source_state") or "") == "degraded"
            syn_fail = ("synoptic" in missing) or degraded
            provider_used_3d = str(quality.get("synoptic_provider_used") or bundle.synoptic_provider_used)
            provider_requested_3d = str(quality.get("synoptic_provider_requested") or bundle.synoptic_provider_used)
            if provider_requested_3d and provider_requested_3d != provider_used_3d:
                header_lines.append(
                    f"⚠️ 数据提醒：数值预报场已从 {provider_requested_3d} 回退到 {provider_used_3d}。"
                )
            elif syn_fail and bundle.synoptic_error:
                header_lines.append(
                    f"⚠️ 数据提醒：数值预报场({provider_used_3d}) 层存在降级，部分环流诊断可能偏弱。"
                )
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

    def _mark(stage: str, seconds: float) -> None:
        perf_local[stage] = float(seconds)
        _perf_log(stage, seconds)

    _prune_runtime_cache()
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
        model = "gfs"

    _send_pending_look_notice(st, target_date)

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
            return runtime_control.deliver_unchanged_notice(cached_payload, notice=unchanged_notice)
    preflight = runtime_control.preflight()
    if not preflight.proceed:
        return str(preflight.text or "")

    # 统一输出：固定走当前主报告链路，对外命令仅暴露 station/date。
    try:
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

        result_text = f"{header}\n\n{bundle.body}\n{bundle.footer}"
        runtime_control.success(
            result_text,
            result_meta=build_cached_result_meta(
                icao=st.icao,
                model=bundle.model,
                metar24=bundle.metar24,
            ),
        )
        return result_text
    except Exception:
        runtime_control.failure()
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
