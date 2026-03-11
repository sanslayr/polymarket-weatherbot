import json
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


class LookGroupPolicyTests(unittest.TestCase):
    def test_defaults_favor_group_only_adaptive_per_group_cooldown(self) -> None:
        policy = look_group_policy.resolve_look_group_policy("-1003586303099")
        self.assertEqual(policy.rate_limit.result_scope, "group-only")
        self.assertEqual(policy.rate_limit.inflight_wait_sec, 3)
        self.assertEqual(policy.rate_limit.user_cooldown.mode, "adaptive")
        self.assertEqual(policy.rate_limit.user_cooldown.scope, "sender-per-group")
        self.assertEqual(policy.rate_limit.user_cooldown.base_sec, 15)
        self.assertEqual(policy.rate_limit.user_cooldown.step_sec, 15)

    def test_invalid_nested_config_falls_back_to_adaptive_defaults(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "look_group_policy.json"
            config_path.write_text(
                json.dumps(
                    {
                        "defaults": {
                            "rate_limit": {
                                "user_cooldown": {
                                    "mode": "invalid-mode",
                                    "scope": "invalid-scope",
                                    "base_sec": "invalid-base",
                                },
                                "result_scope": "invalid-scope",
                                "inflight_wait_sec": "invalid-wait",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            with patch.object(look_group_policy, "CONFIG_PATH", config_path):
                policy = look_group_policy.resolve_look_group_policy("-1003586303099")

        self.assertEqual(policy.rate_limit.result_scope, "group-only")
        self.assertEqual(policy.rate_limit.inflight_wait_sec, 3)
        self.assertEqual(policy.rate_limit.user_cooldown.mode, "adaptive")
        self.assertEqual(policy.rate_limit.user_cooldown.scope, "sender-per-group")
        self.assertEqual(policy.rate_limit.user_cooldown.base_sec, 15)

    def test_legacy_fixed_cooldown_fields_still_parse(self) -> None:
        with TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "look_group_policy.json"
            config_path.write_text(
                json.dumps(
                    {
                        "defaults": {
                            "rate_limit": {
                                "user_cooldown_sec": 45,
                                "user_cooldown_scope": "sender-global",
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            with patch.object(look_group_policy, "CONFIG_PATH", config_path):
                policy = look_group_policy.resolve_look_group_policy("-1003586303099")

        self.assertEqual(policy.rate_limit.user_cooldown.mode, "fixed")
        self.assertEqual(policy.rate_limit.user_cooldown.fixed_sec, 45)
        self.assertEqual(policy.rate_limit.user_cooldown.scope, "sender-global")


if __name__ == "__main__":
    unittest.main()
