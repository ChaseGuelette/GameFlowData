Create a file called _glm_test_output.py with this function:

def calculate_edge(model_prob: float, market_prob: float, fee: float = 0.02) -> float:
    """Calculate fee-adjusted edge."""
    raw_edge = model_prob - market_prob
    return raw_edge - fee

Include a main block that prints calculate_edge(0.65, 0.55).
