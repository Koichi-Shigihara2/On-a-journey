import pandas as pd


def detect_spikes(df: pd.DataFrame,
                  ret_threshold: float = 0.07,
                  vol_ratio_threshold: float = 2.0) -> pd.DataFrame:
    df = df.copy()
    df["return"] = df["close"].pct_change()
    df["vol_ma20"] = df["volume"].rolling(20).mean()
    df["vol_ratio"] = df["volume"] / df["vol_ma20"]
    cond = (df["return"].abs() >= ret_threshold) | (df["vol_ratio"] >= vol_ratio_threshold)
    spikes = df[cond].dropna(subset=["return", "vol_ratio"])
    return spikes
