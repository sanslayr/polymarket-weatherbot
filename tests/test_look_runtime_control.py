import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import look_group_policy  # noqa: E402
import look_runtime_control  # noqa: E402


class LookRuntimeControlTests(unittest.TestCase):
    def _adaptive_policy(self, *, scope: str = "sender-per-group") -> look_group_policy.LookGroupPolicy:
        return look_group_policy.LookGroupPolicy(
            policy_id="-1003586303099",
            rate_limit=look_group_policy.RateLimitPolicy(
                enabled=True,
                apply_in_direct=False,
                user_cooldown=look_group_policy.UserCooldownPolicy(
                    mode="adaptive",
                    scope=scope,
                    fixed_sec=60,
                    base_sec=15,
                    step_sec=15,
                    max_sec=90,
                    window_sec=180,
                    burst_soft_limit=1,
                ),
                result_scope="group-only",
                inflight_wait_sec=3,
                inflight_stale_sec=120,
            ),
            raw={},
        )

    def test_group_without_confirmed_delivery_replays_full_cached_report(self) -> None:
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            pending_dir = state_dir / "pending-deliveries"
            report_dir = state_dir / "report-refs"
            with patch.object(look_runtime_control, "STATE_DIR", state_dir), \
                patch.object(look_runtime_control, "PENDING_DELIVERY_DIR", pending_dir), \
                patch.object(look_runtime_control, "REPORT_REF_DIR", report_dir):
                controller = look_runtime_control.LookRuntimeController(
                    context=look_runtime_control.LookRuntimeContext(
                        channel="telegram",
                        peer_kind="group",
                        peer_id="-1003586303099",
                        sender_id="264157510",
                        session_key="agent:weathernerd:telegram:group:-1003586303099",
                    ),
                    compute_key="ankara-20260310",
                    query_label="Ankara(LTAC)-20260310",
                )
                controller.success("FULL REPORT")
                payload = controller.peek_cached_result_payload()
                self.assertIsNotNone(payload)
                result = controller.deliver_unchanged_notice(payload, notice="NOTICE ONLY")

        self.assertEqual(result, "FULL REPORT")

    def test_group_with_confirmed_delivery_returns_notice(self) -> None:
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            pending_dir = state_dir / "pending-deliveries"
            report_dir = state_dir / "report-refs"
            with patch.object(look_runtime_control, "STATE_DIR", state_dir), \
                patch.object(look_runtime_control, "PENDING_DELIVERY_DIR", pending_dir), \
                patch.object(look_runtime_control, "REPORT_REF_DIR", report_dir):
                controller = look_runtime_control.LookRuntimeController(
                    context=look_runtime_control.LookRuntimeContext(
                        channel="telegram",
                        peer_kind="group",
                        peer_id="-1003586303099",
                        sender_id="264157510",
                        session_key="agent:weathernerd:telegram:group:-1003586303099",
                    ),
                    compute_key="ankara-20260310",
                    query_label="Ankara(LTAC)-20260310",
                )
                controller.success("FULL REPORT")
                payload = controller.peek_cached_result_payload()
                self.assertIsNotNone(payload)
                report_ref_path = look_runtime_control._report_ref_path("-1003586303099", "ankara-20260310")
                report_ref_path.parent.mkdir(parents=True, exist_ok=True)
                look_runtime_control._write_json_atomic(
                    report_ref_path,
                    {
                        "channel": "telegram",
                        "peer_id": "-1003586303099",
                        "compute_key": "ankara-20260310",
                        "payload_updated_at": payload["updated_at"],
                        "report_message_id": "19727",
                    },
                )
                result = controller.deliver_unchanged_notice(payload, notice="NOTICE ONLY")

        self.assertEqual(result, "NOTICE ONLY")

    def test_group_pending_delivery_replays_full_cached_report(self) -> None:
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            pending_dir = state_dir / "pending-deliveries"
            report_dir = state_dir / "report-refs"
            with patch.object(look_runtime_control, "STATE_DIR", state_dir), \
                patch.object(look_runtime_control, "PENDING_DELIVERY_DIR", pending_dir), \
                patch.object(look_runtime_control, "REPORT_REF_DIR", report_dir):
                controller = look_runtime_control.LookRuntimeController(
                    context=look_runtime_control.LookRuntimeContext(
                        channel="telegram",
                        peer_kind="group",
                        peer_id="-1003586303099",
                        sender_id="264157510",
                        session_key="agent:weathernerd:telegram:group:-1003586303099",
                    ),
                    compute_key="ankara-20260310",
                    query_label="Ankara(LTAC)-20260310",
                )
                controller.success("FULL REPORT")
                payload = controller.peek_cached_result_payload()
                self.assertIsNotNone(payload)
                controller._mark_delivery_for_current_chat(payload)
                result = controller.deliver_unchanged_notice(payload, notice="NOTICE ONLY")

        self.assertEqual(result, "FULL REPORT")

    def test_group_only_emits_unchanged_notice_for_same_group_confirmed_payload(self) -> None:
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            pending_dir = state_dir / "pending-deliveries"
            report_dir = state_dir / "report-refs"
            with patch.object(look_runtime_control, "STATE_DIR", state_dir), \
                patch.object(look_runtime_control, "PENDING_DELIVERY_DIR", pending_dir), \
                patch.object(look_runtime_control, "REPORT_REF_DIR", report_dir):
                controller = look_runtime_control.LookRuntimeController(
                    context=look_runtime_control.LookRuntimeContext(
                        channel="telegram",
                        peer_kind="group",
                        peer_id="-1003586303099",
                        sender_id="264157510",
                        session_key="agent:weathernerd:telegram:group:-1003586303099",
                    ),
                    compute_key="tokyo-20260311",
                    query_label="Tokyo(RJTT)-20260311",
                )
                payload = {
                    "text": "FULL REPORT",
                    "updated_at": 1234567890.0,
                    "source_peer_id": "-1001234567890",
                }

                self.assertFalse(controller.should_emit_unchanged_notice(payload))

    def test_group_does_not_emit_unchanged_notice_without_delivery_marker(self) -> None:
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            pending_dir = state_dir / "pending-deliveries"
            report_dir = state_dir / "report-refs"
            with patch.object(look_runtime_control, "STATE_DIR", state_dir), \
                patch.object(look_runtime_control, "PENDING_DELIVERY_DIR", pending_dir), \
                patch.object(look_runtime_control, "REPORT_REF_DIR", report_dir):
                controller = look_runtime_control.LookRuntimeController(
                    context=look_runtime_control.LookRuntimeContext(
                        channel="telegram",
                        peer_kind="group",
                        peer_id="-1003586303099",
                        sender_id="264157510",
                        session_key="agent:weathernerd:telegram:group:-1003586303099",
                    ),
                    compute_key="tokyo-20260311",
                    query_label="Tokyo(RJTT)-20260311",
                )
                payload = {
                    "text": "FULL REPORT",
                    "updated_at": 1234567890.0,
                    "source_peer_id": "-1003586303099",
                }
                report_ref_path = look_runtime_control._report_ref_path("-1003586303099", "tokyo-20260311")
                report_ref_path.parent.mkdir(parents=True, exist_ok=True)
                look_runtime_control._write_json_atomic(
                    report_ref_path,
                    {
                        "channel": "telegram",
                        "peer_id": "-1003586303099",
                        "compute_key": "tokyo-20260311",
                        "payload_updated_at": payload["updated_at"],
                        "report_message_id": "19727",
                    },
                )

                self.assertFalse(controller.should_emit_unchanged_notice(payload))

    def test_group_emits_unchanged_notice_after_same_session_received_payload(self) -> None:
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            pending_dir = state_dir / "pending-deliveries"
            report_dir = state_dir / "report-refs"
            with patch.object(look_runtime_control, "STATE_DIR", state_dir), \
                patch.object(look_runtime_control, "PENDING_DELIVERY_DIR", pending_dir), \
                patch.object(look_runtime_control, "REPORT_REF_DIR", report_dir):
                controller = look_runtime_control.LookRuntimeController(
                    context=look_runtime_control.LookRuntimeContext(
                        channel="telegram",
                        peer_kind="group",
                        peer_id="-1003586303099",
                        sender_id="264157510",
                        session_key="agent:weathernerd:telegram:group:-1003586303099",
                    ),
                    compute_key="tokyo-20260311",
                    query_label="Tokyo(RJTT)-20260311",
                )
                controller.success("FULL REPORT")
                payload = controller.peek_cached_result_payload()
                self.assertIsNotNone(payload)
                report_ref_path = look_runtime_control._report_ref_path("-1003586303099", "tokyo-20260311")
                report_ref_path.parent.mkdir(parents=True, exist_ok=True)
                look_runtime_control._write_json_atomic(
                    report_ref_path,
                    {
                        "channel": "telegram",
                        "peer_id": "-1003586303099",
                        "compute_key": "tokyo-20260311",
                        "payload_updated_at": payload["updated_at"],
                        "report_message_id": "19727",
                    },
                )
                self.assertTrue(controller.should_emit_unchanged_notice(payload))

    def test_adaptive_cooldown_escalates_for_bursty_user(self) -> None:
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            pending_dir = state_dir / "pending-deliveries"
            report_dir = state_dir / "report-refs"
            with patch.object(look_runtime_control, "STATE_DIR", state_dir), \
                patch.object(look_runtime_control, "PENDING_DELIVERY_DIR", pending_dir), \
                patch.object(look_runtime_control, "REPORT_REF_DIR", report_dir), \
                patch.object(look_runtime_control, "resolve_look_group_policy", return_value=self._adaptive_policy()):
                controller = look_runtime_control.LookRuntimeController(
                    context=look_runtime_control.LookRuntimeContext(
                        channel="telegram",
                        peer_kind="group",
                        peer_id="-1003586303099",
                        sender_id="264157510",
                        session_key="agent:weathernerd:telegram:group:-1003586303099",
                    ),
                    compute_key="osaka-20260311",
                    query_label="Osaka(RJOO)-20260311",
                )
                user_state_path = look_runtime_control._user_state_path(controller._user_scope_key())
                look_runtime_control._write_json_atomic(
                    user_state_path,
                    {
                        "sender_scope": controller._user_scope_key(),
                        "last_started_at": controller.now - 5,
                        "recent_started_at": [controller.now - 25, controller.now - 5],
                    },
                )

                decision = controller.preflight()

        self.assertFalse(decision.proceed)
        self.assertIn("动态冷却剩余 25 秒", str(decision.text))
        self.assertIn("当前冷却档位 30 秒", str(decision.text))

    def test_adaptive_cooldown_is_scoped_per_group(self) -> None:
        with TemporaryDirectory() as tmp:
            state_dir = Path(tmp)
            pending_dir = state_dir / "pending-deliveries"
            report_dir = state_dir / "report-refs"
            with patch.object(look_runtime_control, "STATE_DIR", state_dir), \
                patch.object(look_runtime_control, "PENDING_DELIVERY_DIR", pending_dir), \
                patch.object(look_runtime_control, "REPORT_REF_DIR", report_dir), \
                patch.object(look_runtime_control, "resolve_look_group_policy", return_value=self._adaptive_policy()):
                controller_group_a = look_runtime_control.LookRuntimeController(
                    context=look_runtime_control.LookRuntimeContext(
                        channel="telegram",
                        peer_kind="group",
                        peer_id="-1003586303099",
                        sender_id="264157510",
                        session_key="agent:weathernerd:telegram:group:-1003586303099",
                    ),
                    compute_key="ankara-20260311",
                    query_label="Ankara(LTAC)-20260311",
                )
                controller_group_a._touch_user_state()

                controller_group_b = look_runtime_control.LookRuntimeController(
                    context=look_runtime_control.LookRuntimeContext(
                        channel="telegram",
                        peer_kind="group",
                        peer_id="-1004000000000",
                        sender_id="264157510",
                        session_key="agent:weathernerd:telegram:group:-1004000000000",
                    ),
                    compute_key="tokyo-20260311",
                    query_label="Tokyo(RJTT)-20260311",
                )
                cooldown = controller_group_b._user_cooldown_status()

        self.assertEqual(cooldown.remaining_sec, 0)


if __name__ == "__main__":
    unittest.main()
