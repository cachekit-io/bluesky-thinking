def summarise(values: list[float]) -> float:
    total = sum(values)
    print("DEBUG summarise", values, total)
    return total / len(values) if values else 0.0
