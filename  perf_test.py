import logging
import time
import statistics
import pandas as pd
from main import SalesforceAccountFetcher
from account_fields import AccountFields


# --- Logging config ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class SalesforcePerfTester:
    """Performance benchmark for SalesforceAccountFetcher."""

    def __init__(self):
        logging.info("Initializing performance tester...")
        self.fetcher = SalesforceAccountFetcher()
        self.fields = AccountFields().all

    def _run_once(self, field_subset, record_limit=None):
        """Execute one query and measure duration."""
        start = time.perf_counter()
        accounts = self.fetcher.get_all_accounts(fields=field_subset, limit=record_limit)
        duration = time.perf_counter() - start
        return duration, len(accounts)

    def test_field_scaling(self, record_limit=1000):
        """Test how query duration scales with number of fields."""
        logging.info("Starting field scaling test (limit %d records)...", record_limit)

        subsets = [
            ("10 fields", self.fields[:10]),
            ("50 fields", self.fields[:50]),
            ("100 fields", self.fields[:100]),
            ("200 fields", self.fields[:200]),
        ]

        results = []
        for label, subset in subsets:
            logging.info("Testing %s...", label)
            dur, count = self._run_once(subset, record_limit)
            results.append((label, len(subset), dur, count))
            logging.info("%s fetched %d records in %.2fs", label, count, dur)

        df = pd.DataFrame(results, columns=["Label", "Fields", "Time (s)", "Records"])
        logging.info("\n%s", df.to_string(index=False))
        return df

    def test_repeatability(self, field_count=50, repeats=3, record_limit=1000):
        """Test stability of repeated queries with the same field count."""
        logging.info("Starting repeatability test (%d fields, %d repeats)...", field_count, repeats)
        subset = self.fields[:field_count]
        times = []

        for i in range(repeats):
            dur, count = self._run_once(subset, record_limit)
            logging.info("Run %d/%d: %d records in %.2fs", i + 1, repeats, count, dur)
            times.append(dur)

        avg = statistics.mean(times)
        stdev = statistics.stdev(times) if len(times) > 1 else 0.0
        logging.info("Average: %.2fs | StdDev: %.2fs", avg, stdev)
        return avg, stdev

    def test_large_query(self, field_count=200, record_limit=5000):
        """Run a large query to stress-test performance."""
        logging.info("Starting large query test (%d fields, %d records)...", field_count, record_limit)
        subset = self.fields[:field_count]
        start = time.perf_counter()
        accounts = self.fetcher.get_all_accounts(fields=subset, limit=record_limit)
        duration = time.perf_counter() - start
        count = len(accounts)
        logging.info("Fetched %d records in %.2fs", count, duration)
        return duration, count


if __name__ == "__main__":
    tester = SalesforcePerfTester()

    logging.info("=== Salesforce Performance Benchmark ===")
    logging.info("1️⃣  Field scaling test")
    df_scale = tester.test_field_scaling(record_limit=1000)

    logging.info("\n2️⃣  Repeatability test")
    tester.test_repeatability(field_count=50, repeats=3, record_limit=1000)

    logging.info("\n3️⃣  Large query stress test")
    tester.test_large_query(field_count=200, record_limit=5000)

    logging.info("\n✅ All performance tests complete.")
