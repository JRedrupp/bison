"""Shared benchmarking infrastructure for bison bench_*.mojo files.

Provides:
  - BenchResult  — holds timing data for one operation
  - time_fn      — times a Python callable using timeit, returns mean ms
  - print_json   — serialises List[BenchResult] to stdout as JSON
"""

from std.python import Python, PythonObject


struct BenchResult(Copyable, Movable):
    """Timing result for a single benchmark operation."""

    var name: String
    var bison_ms: Float64
    var pandas_ms: Float64
    var iterations: Int
    var skipped: Bool

    def __init__(
        out self,
        name: String,
        bison_ms: Float64,
        pandas_ms: Float64,
        iterations: Int,
    ):
        self.name = name
        self.bison_ms = bison_ms
        self.pandas_ms = pandas_ms
        self.iterations = iterations
        self.skipped = False

    def copy(self) -> BenchResult:
        var r = BenchResult(
            self.name,
            self.bison_ms,
            self.pandas_ms,
            self.iterations,
        )
        r.skipped = self.skipped
        return r^

    @staticmethod
    def skipped_result(name: String) -> BenchResult:
        """Return a placeholder result for a skipped (stub) operation."""
        var r = BenchResult(name, 0.0, 0.0, 0)
        r.skipped = True
        return r^


def time_fn(callable: PythonObject, iterations: Int = 100) raises -> Float64:
    """Time a Python callable using timeit.

    Runs `callable` `iterations` times and returns the mean wall time in
    milliseconds.  Both bison and pandas operations should be wrapped as
    zero-argument Python callables (e.g. ``lambda: df.sum()``) and passed to
    this function so the comparison is apples-to-apples.
    """
    var timeit = Python.import_module("timeit")
    # timeit.timeit returns *total* time in seconds for `number` executions.
    var total_seconds = atof(String(timeit.timeit(callable, number=iterations)))
    return (total_seconds / Float64(iterations)) * 1000.0


def print_json(results: List[BenchResult]) raises:
    """Serialise a List[BenchResult] to a JSON object on stdout.

    Output format::

        {
          "results": [
            {
              "name": "groupby_sum",
              "bison_ms": 1.23,
              "pandas_ms": 0.98,
              "ratio": 1.26,
              "iterations": 100,
              "skipped": false
            }
          ]
        }

    ``ratio`` = bison_ms / pandas_ms; values below 1.0 mean bison is faster.
    """
    var json = Python.import_module("json")
    var py_none = Python.evaluate("None")
    var py_list = Python.evaluate("[]")
    for i in range(len(results)):
        var r = results[i].copy()
        var entry = Python.evaluate("{}")
        entry["name"] = r.name
        entry["skipped"] = r.skipped
        entry["iterations"] = r.iterations
        if r.skipped:
            entry["bison_ms"] = py_none
            entry["pandas_ms"] = py_none
            entry["ratio"] = py_none
        else:
            entry["bison_ms"] = r.bison_ms
            entry["pandas_ms"] = r.pandas_ms
            if r.pandas_ms > 0.0:
                entry["ratio"] = r.bison_ms / r.pandas_ms
            else:
                entry["ratio"] = py_none
        _ = py_list.append(entry)
    var output = Python.evaluate("{}")
    output["results"] = py_list
    print(String(json.dumps(output, indent=2)))
