import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import look_runtime_control  # noqa: E402


class LookRuntimeControlTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
