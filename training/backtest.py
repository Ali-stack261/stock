import pandas as pd

def generate_signals(predictions_df: pd.DataFrame) -> pd.DataFrame:
    """Predicted return > 0 -> BUY, < 0 -> SELL. Adds a 'signal' column."""
    predictions_df["signal"] = predictions_df["predicted_return"].apply(
        lambda r: 1 if r > 0 else -1
    )
    return predictions_df

def compute_directional_accuracy(predictions_df: pd.DataFrame) -> float:
    """% of predictions where sign(predicted_return) == sign(realized_return).
    This is Priority 5's missing metric — and it's the actual input the
    backtest's BUY/SELL logic depends on, not a separate concern."""
    correct = (
        (predictions_df["predicted_return"] > 0) == (predictions_df["realized_return"] > 0)
    )
    return correct.mean()

def run_backtest(predictions_df: pd.DataFrame, initial_capital: float = 10000.0) -> dict:
    """Simulate: signal=1 -> long the realized return, signal=-1 -> short it.
    Returns total return, Sharpe, max drawdown, win rate, profit factor."""
    if len(predictions_df) == 0:
         return {
            "total_return": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "directional_accuracy": 0.0,
            "n_trades": 0,
        }
    
    df = generate_signals(predictions_df.copy())
    df["strategy_return"] = df["signal"] * df["realized_return"]
    df["equity"] = initial_capital * (1 + df["strategy_return"]).cumprod()

    total_return = (df["equity"].iloc[-1] / initial_capital) - 1
    sharpe = (
        df["strategy_return"].mean() / df["strategy_return"].std() * (252 ** 0.5)
        if df["strategy_return"].std() > 0 else 0.0
    )
    running_max = df["equity"].cummax()
    drawdown = (df["equity"] - running_max) / running_max
    max_drawdown = drawdown.min()

    wins = df[df["strategy_return"] > 0]
    losses = df[df["strategy_return"] < 0]
    win_rate = len(wins) / len(df) if len(df) > 0 else 0.0
    profit_factor = (
        wins["strategy_return"].sum() / abs(losses["strategy_return"].sum())
        if len(losses) > 0 and losses["strategy_return"].sum() != 0 else float("inf")
    )

    return {
        "total_return": total_return,
        "sharpe_ratio": sharpe,
        "max_drawdown": max_drawdown,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "directional_accuracy": compute_directional_accuracy(df),
        "n_trades": len(df),
    }

def buy_and_hold_return(predictions_df: pd.DataFrame) -> float:
    """What you'd get just holding the asset the whole period, no signals at all."""
    if len(predictions_df) == 0:
        return 0.0
    return (1 + predictions_df["realized_return"]).prod() - 1
