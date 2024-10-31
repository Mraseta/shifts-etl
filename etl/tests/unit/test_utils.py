import json
import unittest
from unittest.mock import MagicMock, patch

from src.utils import fetch_shifts


class TestFetchShifts(unittest.TestCase):
    @patch("requests.get")
    def test_fetch_shifts(self, mock_get):
        with open("tests/data/mock_shifts_data.json") as f:
            mock_response_data = json.load(f)

        mock_get.return_value = MagicMock(
            status_code=200, json=lambda: mock_response_data
        )

        result = fetch_shifts()

        self.assertTrue(mock_get.called)
        self.assertEqual(len(result), len(mock_response_data["results"]))

        self.assertEqual(result, mock_response_data["results"])

    @patch("requests.get")
    def test_fetch_shifts_error(self, mock_get):
        mock_get.return_value = MagicMock(status_code=500, text="Internal Server Error")

        with self.assertRaises(Exception) as context:
            fetch_shifts()

        self.assertIn("Error fetching shifts.", str(context.exception))


if __name__ == "__main__":
    unittest.main()
