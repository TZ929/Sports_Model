import logging

logger = logging.getLogger(__name__)

def american_to_implied_probability(american_odds: int) -> float:
    """
    Converts American odds to implied probability.

    Args:
        american_odds: The American odds (e.g., -110, +120).

    Returns:
        The implied probability as a float (e.g., 0.5238 for -110).
    """
    if american_odds > 0:
        # Positive odds: probability = 100 / (odds + 100)
        probability = 100 / (american_odds + 100)
    elif american_odds < 0:
        # Negative odds: probability = |odds| / (|odds| + 100)
        probability = abs(american_odds) / (abs(american_odds) + 100)
    else:
        # Even odds (e.g., 0 or 100) are typically not used, but can be treated as 50%
        logger.warning("Received even odds (0 or 100), which is unusual. Assuming 50% probability.")
        return 0.5
    
    return probability

def find_value_opportunity(
    model_prob_over: float, 
    over_odds: int, 
    under_odds: int, 
    edge_threshold: float = 0.05
) -> tuple[str, float] | None:
    """
    Determines if there is a value betting opportunity for OVER or UNDER.

    Args:
        model_prob_over: The model's predicted probability for the OVER outcome.
        over_odds: The American odds for the OVER bet.
        under_odds: The American odds for the UNDER bet.
        edge_threshold: The minimum percentage edge required.

    Returns:
        A tuple of ('OVER' or 'UNDER', edge) or None if no value is found.
    """
    # Check for value on the OVER bet
    implied_prob_over = american_to_implied_probability(over_odds)
    edge_over = model_prob_over - implied_prob_over
    
    if edge_over > edge_threshold:
        return 'OVER', edge_over
        
    # Check for value on the UNDER bet
    model_prob_under = 1 - model_prob_over
    implied_prob_under = american_to_implied_probability(under_odds)
    edge_under = model_prob_under - implied_prob_under
    
    if edge_under > edge_threshold:
        return 'UNDER', edge_under
        
    return None

def main():
    """Example usage for the odds conversion utility."""
    logging.basicConfig(level=logging.INFO)

    # --- Test Cases ---
    odds_list = [-110, 120, -200, 300, -105]
    expected_probs = [0.5238, 0.4545, 0.6667, 0.25, 0.5122]

    for i, odds in enumerate(odds_list):
        prob = american_to_implied_probability(odds)
        print(f"Odds: {odds}, Implied Probability: {prob:.4f} (Expected: {expected_probs[i]})")
        assert abs(prob - expected_probs[i]) < 0.0001, "Test case failed!"

    print("\nAll american_to_implied_probability tests passed!")

    # --- Value Opportunity Test ---
    model_prob_over = 0.60  # Our model gives a 60% chance of OVER.
    over_odds, under_odds = -125, 105
    implied_over_prob = american_to_implied_probability(over_odds)

    value_bet = find_value_opportunity(model_prob_over, over_odds, under_odds)
    print(f"\n--- Test Case 1: Value on OVER ---")
    print(f"Model Prob (Over): {model_prob_over:.2%}, Implied Prob (Over): {implied_over_prob:.2%}")
    if value_bet:
        bet_type, edge = value_bet
        print(f"Value opportunity found: Bet the {bet_type} with an edge of {edge:.2%}")
        assert bet_type == 'OVER'
    else:
        print("No value opportunity found.")
        
    # --- Test Case 2: Value on UNDER ---
    model_prob_over_under = 0.40 # Our model gives a 40% chance of OVER (60% for UNDER)
    over_odds_under, under_odds_under = -110, -110
    implied_under_prob = american_to_implied_probability(under_odds_under)

    value_bet_under = find_value_opportunity(model_prob_over_under, over_odds_under, under_odds_under)
    print(f"\n--- Test Case 2: Value on UNDER ---")
    print(f"Model Prob (Under): {1-model_prob_over_under:.2%}, Implied Prob (Under): {implied_under_prob:.2%}")
    if value_bet_under:
        bet_type, edge = value_bet_under
        print(f"Value opportunity found: Bet the {bet_type} with an edge of {edge:.2%}")
        assert bet_type == 'UNDER'
    else:
        print("No value opportunity found.")
        
    # --- Test Case 3: No Value ---
    model_prob_no_value = 0.53
    value_bet_none = find_value_opportunity(model_prob_no_value, over_odds, under_odds)
    print(f"\n--- Test Case 3: No Value ---")
    if value_bet_none:
        bet_type, edge = value_bet_none
        print(f"Value opportunity found: Bet the {bet_type} with an edge of {edge:.2%}")
    else:
        print("No value opportunity found.")
        assert value_bet_none is None

    print("\nAll find_value_opportunity tests passed!")


if __name__ == '__main__':
    main() 