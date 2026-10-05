import time


def test_round_trip_timing():
    start = time.perf_counter()
    total = sum(range(10_000))
    elapsed_ms = (time.perf_counter() - start) * 1000
    print(f"round trip: {elapsed_ms:.3f} ms")
    assert total == 49_995_000
