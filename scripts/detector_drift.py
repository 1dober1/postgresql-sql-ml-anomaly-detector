def update_streak(bad_runs_streak: int, real_alerts_sent: int, consecutive_limit: int):
    """Дрейф считаем отдельно."""
    if real_alerts_sent > 0:
        bad_runs_streak += 1
    else:
        bad_runs_streak = 0
    drift = bad_runs_streak >= consecutive_limit
    return bad_runs_streak, drift
