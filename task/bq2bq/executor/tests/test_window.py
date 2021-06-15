from unittest import TestCase

from datetime import datetime
from datetime import timedelta

from bumblebee.window import WindowFactory

class test_Window(TestCase):

    def setUp(self):
        self.scheduled_at = datetime(2020, 7, 8, 4)
        self.scheduled_next_at = datetime(2020, 7, 9, 4)

    def test_provide_date_one_and_date_zero_with_window_size(self):
        window = WindowFactory.create_window(self.scheduled_next_at, "24h", "", "")

        day_one = self.scheduled_next_at
        day_zero = day_one - timedelta(days=1)

        self.assertEqual(window.start, day_zero)
        self.assertEqual(window.end, day_one)

    def test_valid_offset_in_window(self):
        window = WindowFactory.create_window(self.scheduled_next_at, "2d", "1d", "")

        day_one = self.scheduled_next_at + timedelta(days=1)
        day_zero = day_one - timedelta(days=2)

        self.assertEqual(window.start, day_zero)
        self.assertEqual(window.end, day_one)

    def test_valid_negative_offset_in_window(self):
        window = WindowFactory.create_window(self.scheduled_next_at, "2d", "-24h", "")

        day_one = self.scheduled_next_at + timedelta(days=-1)
        day_zero = day_one - timedelta(days=2)

        self.assertEqual(window.start, day_zero)
        self.assertEqual(window.end, day_one)

    def test_valid_hour_size_in_window(self):
        window = WindowFactory.create_window(self.scheduled_next_at, "2h", "0", "h")

        day_one = datetime(2020, 7, 9, 4)
        day_zero = datetime(2020, 7, 9, 4) - timedelta(hours=2)

        self.assertEqual(window.start, day_zero)
        self.assertEqual(window.end, day_one)

    def test_valid_truncation_in_window(self):
        window = WindowFactory.create_window(self.scheduled_next_at, "2d", "1d", "d")

        day_one = datetime(2020, 7, 9) + timedelta(days=1)
        day_zero = day_one - timedelta(days=2)

        self.assertEqual(window.start, day_zero)
        self.assertEqual(window.end, day_one)

    def test_valid_week_and_hour_notation(self):
        window = WindowFactory.create_window(self.scheduled_next_at, "1w", "24h", "d")

        day_one = datetime(2020, 7, 9) + timedelta(days=1)
        day_zero = day_one - timedelta(days=7)

        self.assertEqual(window.start, day_zero)
        self.assertEqual(window.end, day_one)

    def test_valid_week_truncation(self):
        window = WindowFactory.create_window(self.scheduled_next_at, "1w", "0", "w")

        day_zero = datetime(2020, 7, 5)
        day_one = datetime(2020, 7, 12)

        self.assertEqual(window.start, day_zero)
        self.assertEqual(window.end, day_one)

    def test_valid_week_from_tuesday_to_tuesday(self):
        window = WindowFactory.create_window(self.scheduled_next_at, "1w", "2d", "w")

        day_zero = datetime(2020, 7, 7)
        day_one = datetime(2020, 7, 14)

        self.assertEqual(window.start, day_zero)
        self.assertEqual(window.end, day_one)


