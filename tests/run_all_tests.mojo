"""Combined test runner — invokes every test suite in a single binary call.

Run with:
    mojo run -I <repo_root> -I <repo_root>/tests tests/run_all_tests.mojo

This file is the entry-point used by scripts/run_tests.sh so that the Mojo
binary is started exactly once instead of once per test file.

To add a new test file, add one import line and one module.main() call below.
Tests within an existing file are auto-discovered via TestSuite.discover_tests,
so no changes to this file are required when adding tests to an existing module.
"""

import test_accessors
import test_aggregation
import test_combining
import test_dataframe
import test_groupby
import test_interop
import test_io
import test_missing
import test_reshaping
import test_series


def main():
    test_accessors.main()
    test_aggregation.main()
    test_combining.main()
    test_dataframe.main()
    test_groupby.main()
    test_interop.main()
    test_io.main()
    test_missing.main()
    test_reshaping.main()
    test_series.main()
    print("")
    print("All test suites passed.")
