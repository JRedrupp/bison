"""Pure sliding-window computation kernels.

Every function operates on List[Float64] + List[Bool] (null mask where
True = null) and returns (List[Float64], List[Bool], Bool) — result data,
result null mask, and whether any output element is null.

No dependency on DataFrame / Series / Column — that wiring lives in
_frame.mojo.
"""

from std.collections import Optional
from std.math import sqrt, exp, log


# ── helpers ──────────────────────────────────────────────────────────


struct WindowResult(Movable):
    """Result of a window computation kernel."""

    var data: List[Float64]
    var mask: List[Bool]
    var has_any_null: Bool

    def __init__(
        out self,
        var data: List[Float64],
        var mask: List[Bool],
        has_any_null: Bool,
    ):
        self.data = data^
        self.mask = mask^
        self.has_any_null = has_any_null


def _nan() -> Float64:
    return Float64(0) / Float64(0)


def _is_null(null_mask: List[Bool], i: Int) -> Bool:
    if len(null_mask) == 0:
        return False
    return null_mask[i]


# ── rolling kernels ──────────────────────────────────────────────────


def rolling_sum(
    data: List[Float64],
    null_mask: List[Bool],
    window: Int,
    min_periods: Int,
) -> WindowResult:
    """O(n) rolling sum with null handling."""
    var n = len(data)
    var nan = _nan()
    var result = List[Float64](capacity=n)
    var result_mask = List[Bool]()
    var has_any_null = False

    var running_sum = Float64(0)
    var valid_count = 0

    for i in range(n):
        # Add entering element
        if not _is_null(null_mask, i):
            running_sum += data[i]
            valid_count += 1

        # Remove leaving element
        if i >= window:
            var leaving = i - window
            if not _is_null(null_mask, leaving):
                running_sum -= data[leaving]
                valid_count -= 1

        # Emit result
        if valid_count < min_periods:
            result.append(nan)
            result_mask.append(True)
            has_any_null = True
        else:
            result.append(running_sum)
            result_mask.append(False)

    return WindowResult(result^, result_mask^, has_any_null)


def rolling_count(
    data: List[Float64],
    null_mask: List[Bool],
    window: Int,
    min_periods: Int,
) -> WindowResult:
    """O(n) rolling count of non-null values."""
    var n = len(data)
    var nan = _nan()
    var result = List[Float64](capacity=n)
    var result_mask = List[Bool]()
    var has_any_null = False

    var valid_count = 0

    for i in range(n):
        if not _is_null(null_mask, i):
            valid_count += 1

        if i >= window:
            var leaving = i - window
            if not _is_null(null_mask, leaving):
                valid_count -= 1

        # count always emits a value (pandas rolling count uses min_periods=0
        # by default, but we respect the caller's min_periods)
        if valid_count < min_periods:
            result.append(nan)
            result_mask.append(True)
            has_any_null = True
        else:
            result.append(Float64(valid_count))
            result_mask.append(False)

    return WindowResult(result^, result_mask^, has_any_null)


def rolling_mean(
    data: List[Float64],
    null_mask: List[Bool],
    window: Int,
    min_periods: Int,
) -> WindowResult:
    """O(n) rolling mean with null handling."""
    var n = len(data)
    var nan = _nan()
    var result = List[Float64](capacity=n)
    var result_mask = List[Bool]()
    var has_any_null = False

    var running_sum = Float64(0)
    var valid_count = 0

    for i in range(n):
        if not _is_null(null_mask, i):
            running_sum += data[i]
            valid_count += 1

        if i >= window:
            var leaving = i - window
            if not _is_null(null_mask, leaving):
                running_sum -= data[leaving]
                valid_count -= 1

        if valid_count < min_periods:
            result.append(nan)
            result_mask.append(True)
            has_any_null = True
        else:
            result.append(running_sum / Float64(valid_count))
            result_mask.append(False)

    return WindowResult(result^, result_mask^, has_any_null)


def rolling_var(
    data: List[Float64],
    null_mask: List[Bool],
    window: Int,
    min_periods: Int,
    ddof: Int = 1,
) -> WindowResult:
    """O(n) rolling variance using running sum and sum-of-squares."""
    var n = len(data)
    var nan = _nan()
    var result = List[Float64](capacity=n)
    var result_mask = List[Bool]()
    var has_any_null = False

    var running_sum = Float64(0)
    var running_sum_sq = Float64(0)
    var valid_count = 0

    for i in range(n):
        if not _is_null(null_mask, i):
            running_sum += data[i]
            running_sum_sq += data[i] * data[i]
            valid_count += 1

        if i >= window:
            var leaving = i - window
            if not _is_null(null_mask, leaving):
                running_sum -= data[leaving]
                running_sum_sq -= data[leaving] * data[leaving]
                valid_count -= 1

        if valid_count < min_periods or valid_count <= ddof:
            result.append(nan)
            result_mask.append(True)
            has_any_null = True
        else:
            var mean = running_sum / Float64(valid_count)
            var variance = (
                (running_sum_sq / Float64(valid_count) - mean * mean)
                * Float64(valid_count)
                / Float64(valid_count - ddof)
            )
            result.append(variance)
            result_mask.append(False)

    return WindowResult(result^, result_mask^, has_any_null)


def rolling_std(
    data: List[Float64],
    null_mask: List[Bool],
    window: Int,
    min_periods: Int,
    ddof: Int = 1,
) -> WindowResult:
    """O(n) rolling standard deviation."""
    var var_result = rolling_var(data, null_mask, window, min_periods, ddof)
    var result = List[Float64](capacity=len(var_result.data))
    for i in range(len(var_result.data)):
        if var_result.mask[i]:
            result.append(var_result.data[i])
        else:
            result.append(sqrt(var_result.data[i]))
    var out_mask = var_result.mask.copy()
    var out_has_null = var_result.has_any_null
    return WindowResult(result^, out_mask^, out_has_null)


def rolling_min(
    data: List[Float64],
    null_mask: List[Bool],
    window: Int,
    min_periods: Int,
) -> WindowResult:
    """O(n) rolling minimum using a monotonic deque."""
    var n = len(data)
    var nan = _nan()
    var result = List[Float64](capacity=n)
    var result_mask = List[Bool]()
    var has_any_null = False

    # Monotonic deque: stores indices of potential minimums in increasing
    # value order. Front of deque is always the current minimum.
    var deque = List[Int]()
    var valid_count = 0

    for i in range(n):
        var is_null_i = _is_null(null_mask, i)

        if not is_null_i:
            valid_count += 1
            # Pop from back while the new value is <= back value
            while len(deque) > 0 and data[deque[len(deque) - 1]] >= data[i]:
                _ = deque.pop()
            deque.append(i)

        # Remove elements that have left the window
        if i >= window:
            var leaving = i - window
            if not _is_null(null_mask, leaving):
                valid_count -= 1
            # Pop front if it has left the window
            while len(deque) > 0 and deque[0] <= i - window:
                # Shift elements left (pop front)
                var new_deque = List[Int]()
                for j in range(1, len(deque)):
                    new_deque.append(deque[j])
                deque = new_deque^

        if valid_count < min_periods:
            result.append(nan)
            result_mask.append(True)
            has_any_null = True
        else:
            if len(deque) > 0:
                result.append(data[deque[0]])
                result_mask.append(False)
            else:
                result.append(nan)
                result_mask.append(True)
                has_any_null = True

    return WindowResult(result^, result_mask^, has_any_null)


def rolling_max(
    data: List[Float64],
    null_mask: List[Bool],
    window: Int,
    min_periods: Int,
) -> WindowResult:
    """O(n) rolling maximum using a monotonic deque."""
    var n = len(data)
    var nan = _nan()
    var result = List[Float64](capacity=n)
    var result_mask = List[Bool]()
    var has_any_null = False

    # Monotonic deque: stores indices of potential maximums in decreasing
    # value order. Front of deque is always the current maximum.
    var deque = List[Int]()
    var valid_count = 0

    for i in range(n):
        var is_null_i = _is_null(null_mask, i)

        if not is_null_i:
            valid_count += 1
            # Pop from back while the new value is >= back value
            while len(deque) > 0 and data[deque[len(deque) - 1]] <= data[i]:
                _ = deque.pop()
            deque.append(i)

        # Remove elements that have left the window
        if i >= window:
            var leaving = i - window
            if not _is_null(null_mask, leaving):
                valid_count -= 1
            while len(deque) > 0 and deque[0] <= i - window:
                var new_deque = List[Int]()
                for j in range(1, len(deque)):
                    new_deque.append(deque[j])
                deque = new_deque^

        if valid_count < min_periods:
            result.append(nan)
            result_mask.append(True)
            has_any_null = True
        else:
            if len(deque) > 0:
                result.append(data[deque[0]])
                result_mask.append(False)
            else:
                result.append(nan)
                result_mask.append(True)
                has_any_null = True

    return WindowResult(result^, result_mask^, has_any_null)


# ── expanding kernels ────────────────────────────────────────────────


def expanding_sum(
    data: List[Float64],
    null_mask: List[Bool],
    min_periods: Int,
) -> WindowResult:
    """Running sum from position 0."""
    var n = len(data)
    var nan = _nan()
    var result = List[Float64](capacity=n)
    var result_mask = List[Bool]()
    var has_any_null = False

    var running_sum = Float64(0)
    var valid_count = 0

    for i in range(n):
        if not _is_null(null_mask, i):
            running_sum += data[i]
            valid_count += 1

        if valid_count < min_periods:
            result.append(nan)
            result_mask.append(True)
            has_any_null = True
        else:
            result.append(running_sum)
            result_mask.append(False)

    return WindowResult(result^, result_mask^, has_any_null)


def expanding_count(
    data: List[Float64],
    null_mask: List[Bool],
    min_periods: Int,
) -> WindowResult:
    """Running count of non-null values from position 0."""
    var n = len(data)
    var nan = _nan()
    var result = List[Float64](capacity=n)
    var result_mask = List[Bool]()
    var has_any_null = False

    var valid_count = 0

    for i in range(n):
        if not _is_null(null_mask, i):
            valid_count += 1

        if valid_count < min_periods:
            result.append(nan)
            result_mask.append(True)
            has_any_null = True
        else:
            result.append(Float64(valid_count))
            result_mask.append(False)

    return WindowResult(result^, result_mask^, has_any_null)


def expanding_mean(
    data: List[Float64],
    null_mask: List[Bool],
    min_periods: Int,
) -> WindowResult:
    """Running mean from position 0."""
    var n = len(data)
    var nan = _nan()
    var result = List[Float64](capacity=n)
    var result_mask = List[Bool]()
    var has_any_null = False

    var running_sum = Float64(0)
    var valid_count = 0

    for i in range(n):
        if not _is_null(null_mask, i):
            running_sum += data[i]
            valid_count += 1

        if valid_count < min_periods:
            result.append(nan)
            result_mask.append(True)
            has_any_null = True
        else:
            result.append(running_sum / Float64(valid_count))
            result_mask.append(False)

    return WindowResult(result^, result_mask^, has_any_null)


def expanding_var(
    data: List[Float64],
    null_mask: List[Bool],
    min_periods: Int,
    ddof: Int = 1,
) -> WindowResult:
    """Running variance from position 0 using Welford's algorithm."""
    var n = len(data)
    var nan = _nan()
    var result = List[Float64](capacity=n)
    var result_mask = List[Bool]()
    var has_any_null = False

    var running_sum = Float64(0)
    var running_sum_sq = Float64(0)
    var valid_count = 0

    for i in range(n):
        if not _is_null(null_mask, i):
            running_sum += data[i]
            running_sum_sq += data[i] * data[i]
            valid_count += 1

        if valid_count < min_periods or valid_count <= ddof:
            result.append(nan)
            result_mask.append(True)
            has_any_null = True
        else:
            var mean = running_sum / Float64(valid_count)
            var variance = (
                (running_sum_sq / Float64(valid_count) - mean * mean)
                * Float64(valid_count)
                / Float64(valid_count - ddof)
            )
            result.append(variance)
            result_mask.append(False)

    return WindowResult(result^, result_mask^, has_any_null)


def expanding_std(
    data: List[Float64],
    null_mask: List[Bool],
    min_periods: Int,
    ddof: Int = 1,
) -> WindowResult:
    """Running standard deviation from position 0."""
    var var_result = expanding_var(data, null_mask, min_periods, ddof)
    var result = List[Float64](capacity=len(var_result.data))
    for i in range(len(var_result.data)):
        if var_result.mask[i]:
            result.append(var_result.data[i])
        else:
            result.append(sqrt(var_result.data[i]))
    var out_mask = var_result.mask.copy()
    var out_has_null = var_result.has_any_null
    return WindowResult(result^, out_mask^, out_has_null)


def expanding_min(
    data: List[Float64],
    null_mask: List[Bool],
    min_periods: Int,
) -> WindowResult:
    """Running minimum from position 0."""
    var n = len(data)
    var nan = _nan()
    var result = List[Float64](capacity=n)
    var result_mask = List[Bool]()
    var has_any_null = False

    var current_min = Float64(0)
    var valid_count = 0

    for i in range(n):
        if not _is_null(null_mask, i):
            valid_count += 1
            if valid_count == 1 or data[i] < current_min:
                current_min = data[i]

        if valid_count < min_periods:
            result.append(nan)
            result_mask.append(True)
            has_any_null = True
        else:
            result.append(current_min)
            result_mask.append(False)

    return WindowResult(result^, result_mask^, has_any_null)


def expanding_max(
    data: List[Float64],
    null_mask: List[Bool],
    min_periods: Int,
) -> WindowResult:
    """Running maximum from position 0."""
    var n = len(data)
    var nan = _nan()
    var result = List[Float64](capacity=n)
    var result_mask = List[Bool]()
    var has_any_null = False

    var current_max = Float64(0)
    var valid_count = 0

    for i in range(n):
        if not _is_null(null_mask, i):
            valid_count += 1
            if valid_count == 1 or data[i] > current_max:
                current_max = data[i]

        if valid_count < min_periods:
            result.append(nan)
            result_mask.append(True)
            has_any_null = True
        else:
            result.append(current_max)
            result_mask.append(False)

    return WindowResult(result^, result_mask^, has_any_null)


# ── EWM kernels ──────────────────────────────────────────────────────


def resolve_ewm_alpha(
    com: Optional[Float64],
    span: Optional[Float64],
    halflife: Optional[Float64],
    alpha: Optional[Float64],
) raises -> Float64:
    """Resolve exactly one of com/span/halflife/alpha to the decay factor.

    Raises if zero or more than one parameter is provided.
    """
    var count = 0
    if com:
        count += 1
    if span:
        count += 1
    if halflife:
        count += 1
    if alpha:
        count += 1

    if count == 0:
        raise Error("ewm: must pass one of com, span, halflife, or alpha")
    if count > 1:
        raise Error("ewm: must pass only one of com, span, halflife, or alpha")

    if com:
        var c = com.value()
        if c < 0:
            raise Error("ewm: com must be >= 0")
        return Float64(1) / (Float64(1) + c)
    if span:
        var s = span.value()
        if s < 1:
            raise Error("ewm: span must be >= 1")
        return Float64(2) / (s + Float64(1))
    if halflife:
        var h = halflife.value()
        if h <= 0:
            raise Error("ewm: halflife must be > 0")
        return Float64(1) - exp(-log(Float64(2)) / h)
    # alpha
    var a = alpha.value()
    if a <= 0 or a > 1:
        raise Error("ewm: alpha must be in (0, 1]")
    return a


def ewm_mean(
    data: List[Float64],
    null_mask: List[Bool],
    alpha: Float64,
) -> WindowResult:
    """Exponentially weighted moving average.

    Uses the pandas "adjust=True" semantics (default):
      numerator[0]   = x[0]
      numerator[i]   = x[i] + (1 - alpha) * numerator[i-1]
      denominator[0] = 1
      denominator[i] = 1 + (1 - alpha) * denominator[i-1]
      ewma[i]        = numerator[i] / denominator[i]

    Null values are skipped (weights carry forward).
    """
    var n = len(data)
    var nan = _nan()
    var result = List[Float64](capacity=n)
    var result_mask = List[Bool]()
    var has_any_null = False

    var one_minus_alpha = Float64(1) - alpha
    var numer = Float64(0)
    var denom = Float64(0)
    var started = False

    for i in range(n):
        if _is_null(null_mask, i):
            if not started:
                result.append(nan)
                result_mask.append(True)
                has_any_null = True
            else:
                # Carry forward: decay numerator and denominator
                numer *= one_minus_alpha
                denom *= one_minus_alpha
                result.append(numer / denom)
                result_mask.append(False)
        else:
            if not started:
                numer = data[i]
                denom = Float64(1)
                started = True
            else:
                numer = data[i] + one_minus_alpha * numer
                denom = Float64(1) + one_minus_alpha * denom
            result.append(numer / denom)
            result_mask.append(False)

    return WindowResult(result^, result_mask^, has_any_null)


def ewm_var(
    data: List[Float64],
    null_mask: List[Bool],
    alpha: Float64,
    ddof: Int = 1,
) -> WindowResult:
    """Exponentially weighted moving variance.

    Uses the recursive formulation:
      mean[i] = ewm_mean at position i
      var[i]  = (1 - alpha) * (var[i-1] + alpha * (x[i] - mean[i-1])^2)

    With bias correction for ddof=1 (default).
    """
    var n = len(data)
    var nan = _nan()
    var result = List[Float64](capacity=n)
    var result_mask = List[Bool]()
    var has_any_null = False

    var one_minus_alpha = Float64(1) - alpha

    # First compute EWM mean at each position
    var ewm_means = ewm_mean(data, null_mask, alpha)

    # Track running weighted variance
    var numer = Float64(0)
    var sum_weights = Float64(0)
    var sum_weights_sq = Float64(0)
    var old_mean = Float64(0)
    var valid_count = 0

    for i in range(n):
        if _is_null(null_mask, i):
            if valid_count < 2:
                result.append(nan)
                result_mask.append(True)
                has_any_null = True
            else:
                # Carry forward previous variance
                result.append(result[i - 1])
                result_mask.append(result_mask[i - 1])
                if result_mask[i - 1]:
                    has_any_null = True
        else:
            valid_count += 1
            if valid_count < 2:
                result.append(nan)
                result_mask.append(True)
                has_any_null = True
                old_mean = data[i]
                sum_weights = Float64(1)
                sum_weights_sq = Float64(1)
            else:
                # Update weighted sums
                sum_weights = Float64(1) + one_minus_alpha * sum_weights
                sum_weights_sq = (
                    Float64(1)
                    + one_minus_alpha * one_minus_alpha * sum_weights_sq
                )
                var new_mean = ewm_means.data[i]
                var diff = data[i] - old_mean
                numer = one_minus_alpha * (numer + alpha * diff * diff)
                old_mean = new_mean

                if ddof == 1:
                    # Unbiased correction
                    var bias_correction = (
                        sum_weights
                        * sum_weights
                        / (sum_weights * sum_weights - sum_weights_sq)
                    )
                    result.append(numer * bias_correction)
                else:
                    result.append(numer)
                result_mask.append(False)

    return WindowResult(result^, result_mask^, has_any_null)


def ewm_std(
    data: List[Float64],
    null_mask: List[Bool],
    alpha: Float64,
    ddof: Int = 1,
) -> WindowResult:
    """Exponentially weighted moving standard deviation."""
    var var_result = ewm_var(data, null_mask, alpha, ddof)
    var result = List[Float64](capacity=len(var_result.data))
    for i in range(len(var_result.data)):
        if var_result.mask[i]:
            result.append(var_result.data[i])
        else:
            result.append(sqrt(var_result.data[i]))
    var out_mask = var_result.mask.copy()
    var out_has_null = var_result.has_any_null
    return WindowResult(result^, out_mask^, out_has_null)
