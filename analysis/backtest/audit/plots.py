"""Matplotlib chart helpers for the Phase A audit.

Every function takes an explicit save path and returns it. Consistent
global style is set once on import.
"""

from __future__ import annotations

import os
from typing import Iterable, Sequence

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

try:
    plt.style.use("seaborn-v0_8-whitegrid")
except OSError:
    plt.style.use("default")

plt.rcParams.update({
    "figure.autolayout": True,
    "axes.titlesize":    11,
    "axes.labelsize":    10,
    "xtick.labelsize":   9,
    "ytick.labelsize":   9,
    "legend.fontsize":   9,
    "font.family":       "DejaVu Sans",
})

_DPI = 150


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _save(fig: plt.Figure, path: str) -> str:
    _ensure_dir(path)
    fig.savefig(path, dpi=_DPI, bbox_inches="tight")
    plt.close(fig)
    return path


def heatmap(matrix: pd.DataFrame, title: str, path: str,
            fmt: str = ".2f", cmap: str = "RdBu_r",
            vmin: float | None = -1.0, vmax: float | None = 1.0) -> str:
    fig, ax = plt.subplots(figsize=(max(5, 0.6 * len(matrix) + 3),
                                    max(4, 0.6 * len(matrix) + 2.5)))
    im = ax.imshow(matrix.values, cmap=cmap, vmin=vmin, vmax=vmax, aspect="auto")
    ax.set_xticks(range(len(matrix.columns)))
    ax.set_xticklabels(matrix.columns, rotation=45, ha="right")
    ax.set_yticks(range(len(matrix.index)))
    ax.set_yticklabels(matrix.index)
    ax.set_title(title)
    fig.colorbar(im, ax=ax, shrink=0.8)
    for i in range(matrix.shape[0]):
        for j in range(matrix.shape[1]):
            val = matrix.values[i, j]
            if pd.isna(val):
                continue
            color = "white" if abs(val) > 0.6 else "black"
            ax.text(j, i, format(val, fmt), ha="center", va="center",
                    color=color, fontsize=8)
    return _save(fig, path)


def histogram_grid(series_map: dict[str, pd.Series], title: str, path: str,
                   bins: int = 30, ncols: int = 3) -> str:
    n = len(series_map)
    ncols = min(ncols, n)
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(4 * ncols, 3 * nrows),
                             squeeze=False)
    for ax, (name, ser) in zip(axes.flatten(), series_map.items()):
        ser = ser.dropna()
        ax.hist(ser.values, bins=bins, edgecolor="black", alpha=0.75)
        ax.set_title(name)
        ax.set_xlabel("score")
        ax.set_ylabel("count")
        if len(ser):
            ax.axvline(ser.mean(), color="red", lw=1, ls="--",
                       label=f"mean={ser.mean():.1f}")
            ax.axvline(ser.median(), color="green", lw=1, ls=":",
                       label=f"median={ser.median():.1f}")
            ax.legend(loc="best")
    for ax in axes.flatten()[n:]:
        ax.set_visible(False)
    fig.suptitle(title)
    return _save(fig, path)


def single_histogram(series: pd.Series, title: str, path: str,
                     bins: int = 40, xlabel: str = "value") -> str:
    fig, ax = plt.subplots(figsize=(7, 4))
    ser = series.dropna()
    ax.hist(ser.values, bins=bins, edgecolor="black", alpha=0.8)
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("count")
    if len(ser):
        ax.axvline(ser.mean(), color="red", lw=1, ls="--",
                   label=f"mean={ser.mean():.2f}")
        ax.axvline(ser.median(), color="green", lw=1, ls=":",
                   label=f"median={ser.median():.2f}")
        ax.legend(loc="best")
    return _save(fig, path)


def bar_chart(labels: Sequence[str], values: Sequence[float], title: str,
              path: str, xlabel: str = "", ylabel: str = "count") -> str:
    fig, ax = plt.subplots(figsize=(max(5, 0.6 * len(labels) + 2), 4))
    ax.bar(labels, values, edgecolor="black")
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.tick_params(axis="x", rotation=30)
    for i, v in enumerate(values):
        label = f"{v:.1f}" if isinstance(v, float) else str(v)
        ax.text(i, v, label, ha="center", va="bottom", fontsize=8)
    return _save(fig, path)


def stacked_bar(df: pd.DataFrame, title: str, path: str,
                xlabel: str = "decile", ylabel: str = "% of decile") -> str:
    fig, ax = plt.subplots(figsize=(max(7, 0.8 * len(df) + 2), 5))
    bottom = np.zeros(len(df))
    colors = plt.colormaps["tab20"].colors
    for i, col in enumerate(df.columns):
        ax.bar(df.index.astype(str), df[col].values, bottom=bottom,
               label=str(col), color=colors[i % len(colors)],
               edgecolor="white", linewidth=0.5)
        bottom += df[col].values
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", borderaxespad=0,
              fontsize=8)
    return _save(fig, path)


def scree_plot(explained_variance_ratio: Iterable[float], title: str,
               path: str) -> str:
    evr = list(explained_variance_ratio)
    fig, ax = plt.subplots(figsize=(6, 4))
    xs = list(range(1, len(evr) + 1))
    ax.bar(xs, evr, edgecolor="black", alpha=0.75, label="per-component")
    cum = np.cumsum(evr)
    ax.plot(xs, cum, marker="o", color="red", label="cumulative")
    ax.set_xticks(xs)
    ax.set_xticklabels([f"PC{i}" for i in xs])
    ax.set_ylim(0, 1.05)
    ax.set_title(title)
    ax.set_ylabel("explained variance ratio")
    for i, (v, c) in enumerate(zip(evr, cum), start=1):
        ax.text(i, v, f"{v:.2f}", ha="center", va="bottom", fontsize=8)
        ax.text(i, c + 0.02, f"{c:.2f}", ha="center", va="bottom",
                fontsize=8, color="red")
    ax.legend()
    return _save(fig, path)


def scatter_grid(pairs: list[tuple[str, pd.Series, pd.Series, str]],
                 title: str, path: str, ncols: int = 2) -> str:
    """pairs: list of (subplot title, x-series, y-series, xlabel)."""
    n = len(pairs)
    if n == 0:
        fig, ax = plt.subplots(figsize=(4, 2))
        ax.text(0.5, 0.5, "no data", ha="center", va="center")
        return _save(fig, path)
    ncols = min(ncols, n)
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows),
                             squeeze=False)
    for ax, (name, x, y, xlabel) in zip(axes.flatten(), pairs):
        m = x.notna() & y.notna()
        ax.scatter(x[m].values, y[m].values, s=6, alpha=0.4)
        ax.set_title(name)
        ax.set_xlabel(xlabel)
        ax.set_ylabel("composite_score")
    for ax in axes.flatten()[n:]:
        ax.set_visible(False)
    fig.suptitle(title)
    return _save(fig, path)
