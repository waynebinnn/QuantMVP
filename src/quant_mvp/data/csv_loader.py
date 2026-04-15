from __future__ import annotations

from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = {"datetime", "open", "high", "low", "close", "volume"}



def load_ohlcv_csv(path: str, datetime_col: str = "datetime") -> pd.DataFrame:
    df = pd.read_csv(Path(path))

    if datetime_col != "datetime":
        if datetime_col not in df.columns:
            raise ValueError(f"Missing datetime column: {datetime_col}")
        df = df.rename(columns={datetime_col: "datetime"})

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").drop_duplicates(subset=["datetime"]).reset_index(drop=True)

    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if df[numeric_cols].isna().any().any():
        bad_rows = df[df[numeric_cols].isna().any(axis=1)]
        raise ValueError(f"Found invalid numeric rows: {len(bad_rows)}")

    return df
