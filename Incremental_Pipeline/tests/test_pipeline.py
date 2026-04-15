"""
test_pipeline.py — Unit tests for the incremental pipeline.
Run with: pytest tests/ -v

These tests use mocks — no real BQ or API calls are made.
"""

import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, call

from utils.watermark import get_watermark, set_watermark
from utils.bq_loader import _normalise_row, _detect_type, _infer_schema
from extractors.zoho_extractor import ZohoExtractor
from extractors.hubspot_extractor import HubSpotExtractor


class TestNormaliseRow(unittest.TestCase):

    def test_dict_values_serialized_to_json(self):
        row = {"owner": {"id": "123", "name": "Alice"}}
        result = _normalise_row(row)
        self.assertIn('"id": "123"', result["owner"])

    def test_list_values_serialized_to_json(self):
        row = {"tags": ["a", "b", "c"]}
        result = _normalise_row(row)
        self.assertEqual(result["tags"], '["a", "b", "c"]')

    def test_column_name_sanitization(self):
        row = {"deal-stage": "closed", "deal.amount": 100, "deal amount": 200}
        result = _normalise_row(row)
        self.assertIn("deal_stage", result)
        self.assertIn("deal_amount", result)

    def test_none_values_preserved(self):
        row = {"id": "1", "amount": None}
        result = _normalise_row(row)
        self.assertIsNone(result["amount"])

    def test_bool_preserved(self):
        row = {"active": True, "archived": False}
        result = _normalise_row(row)
        self.assertTrue(result["active"])
        self.assertFalse(result["archived"])


class TestDetectType(unittest.TestCase):

    def test_bool_detected(self):
        self.assertEqual(_detect_type(True), "BOOLEAN")
        self.assertEqual(_detect_type(False), "BOOLEAN")

    def test_int_detected(self):
        self.assertEqual(_detect_type(42), "INTEGER")

    def test_float_detected(self):
        self.assertEqual(_detect_type(3.14), "FLOAT")

    def test_string_detected(self):
        self.assertEqual(_detect_type("hello"), "STRING")

    def test_dict_becomes_string(self):
        self.assertEqual(_detect_type({"k": "v"}), "STRING")

    def test_list_becomes_string(self):
        self.assertEqual(_detect_type([1, 2, 3]), "STRING")


class TestInferSchema(unittest.TestCase):

    def test_basic_schema_inferred(self):
        records = [{"id": "1", "amount": 100.0, "active": True}]
        schema = _infer_schema(records)
        field_map = {f.name: f.field_type for f in schema}
        self.assertEqual(field_map["id"], "STRING")
        self.assertEqual(field_map["amount"], "FLOAT")
        self.assertEqual(field_map["active"], "BOOLEAN")

    def test_type_conflict_falls_back_to_string(self):
        records = [
            {"value": 42},
            {"value": "text"},
        ]
        schema = _infer_schema(records)
        field_map = {f.name: f.field_type for f in schema}
        self.assertEqual(field_map["value"], "STRING")

    def test_all_none_column_becomes_string(self):
        records = [{"id": "1", "notes": None}, {"id": "2", "notes": None}]
        schema = _infer_schema(records)
        field_map = {f.name: f.field_type for f in schema}
        self.assertIn("notes", field_map)
        self.assertEqual(field_map["notes"], "STRING")


class TestWatermark(unittest.TestCase):

    def _make_mock_client(self, rows=None):
        client = MagicMock()
        client.project = "test-project"
        query_job = MagicMock()
        query_job.result.return_value = rows or []
        client.query.return_value = query_job
        return client

    def test_get_watermark_returns_stored_value(self):
        stored_ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_row = MagicMock()
        mock_row.last_synced_at = stored_ts
        client = self._make_mock_client(rows=[mock_row])

        from config.settings import WATERMARK_OVERLAP_MINUTES
        result = get_watermark(client, "zoho", "Deals")

        expected = stored_ts - timedelta(minutes=WATERMARK_OVERLAP_MINUTES)
        self.assertEqual(result, expected)

    def test_get_watermark_fallback_on_first_run(self):
        client = self._make_mock_client(rows=[])
        result = get_watermark(client, "zoho", "Deals")
        # Should be ~10 years ago
        ten_years_ago = datetime.now(timezone.utc) - timedelta(days=3650)
        diff = abs((result - ten_years_ago).total_seconds())
        self.assertLess(diff, 60)  # Within 1 minute of expected


class TestZohoRevenueDelta(unittest.TestCase):

    def setUp(self):
        self.extractor = ZohoExtractor.__new__(ZohoExtractor)
        self.extractor._call_count = 0
        self.extractor._lock = __import__("threading").Lock()

    def test_new_record_logged_as_create(self):
        records = [{"id": "REV001", "Amount": "5000", "Date": "2024-01-01",
                    "Parent_Id": {"id": "BLK001", "name": "Block A"}}]
        previous = {}
        delta = self.extractor.build_revenue_history_delta(records, previous)
        self.assertEqual(len(delta), 1)
        self.assertEqual(delta[0]["Type"], "Create")

    def test_changed_amount_detected(self):
        records = [{"id": "REV001", "Amount": "7500", "Date": "2024-01-01",
                    "Parent_Id": {"id": "BLK001", "name": "Block A"}}]
        previous = {"REV001": {"amount": "5000", "Revenue_date": "2024-01-01"}}
        delta = self.extractor.build_revenue_history_delta(records, previous)
        self.assertEqual(len(delta), 1)
        self.assertIn("Amount changed", delta[0]["Type"])

    def test_unchanged_record_not_logged(self):
        records = [{"id": "REV001", "Amount": "5000", "Date": "2024-01-01",
                    "Parent_Id": {"id": "BLK001", "name": "Block A"}}]
        previous = {"REV001": {"amount": "5000", "Revenue_date": "2024-01-01"}}
        delta = self.extractor.build_revenue_history_delta(records, previous)
        self.assertEqual(len(delta), 0)

    def test_both_changes_detected(self):
        records = [{"id": "REV001", "Amount": "9000", "Date": "2024-06-01",
                    "Parent_Id": {"id": "BLK001", "name": "Block A"}}]
        previous = {"REV001": {"amount": "5000", "Revenue_date": "2024-01-01"}}
        delta = self.extractor.build_revenue_history_delta(records, previous)
        self.assertEqual(len(delta), 1)
        self.assertIn("Amount changed", delta[0]["Type"])
        self.assertIn("Date changed", delta[0]["Type"])


if __name__ == "__main__":
    unittest.main()
