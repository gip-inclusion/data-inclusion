#!/usr/bin/env python3
"""
Data Quality Comparison Tool
Compare v0 and v1 versions of structures and services tables.
Generates comprehensive reports on data quality improvements.
"""

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class ComparisonConfig:
    """Configuration for table comparison."""

    name: str
    v0_path: Path
    v1_path: Path
    # In v0, (source, id) is unique; in v1, id alone is unique
    id_col: str = "id"
    source_col: str = "source"
    score_col: str = "score_qualite"
    cluster_col: str = "cluster_id"
    # Column mappings: v0_col -> v1_col (if renamed)
    # Example: {"presentation_detail": "description", "types": "type"}
    column_mappings: dict = field(default_factory=dict)

    @classmethod
    def load_mappings_from_json(cls, json_path: Path) -> dict:
        """Load column mappings from a JSON file."""
        import json

        with open(json_path) as f:
            return json.load(f)


@dataclass
class SourceStats:
    """Statistics for a single source."""

    name: str
    v0_count: int = 0
    v1_count: int = 0
    added: int = 0
    removed: int = 0
    kept: int = 0
    # For matched rows only
    changed: int = 0
    unchanged: int = 0


@dataclass
class ColumnStats:
    """Fill rate statistics for a column."""

    name: str
    v0_name: Optional[str] = None  # Original name in v0 if mapped
    v0_fill_rate: float = 0.0
    v1_fill_rate: float = 0.0
    v0_count: int = 0
    v1_count: int = 0

    @property
    def improvement(self) -> float:
        return self.v1_fill_rate - self.v0_fill_rate


def is_filled(value) -> bool:
    """Check if a value is considered 'filled' (not emptyish).

    Returns False for:
    - None/NaN
    - Empty string
    - Empty list/array
    - Empty dict

    Returns True for all other values.
    """
    # Check for numpy array first (pd.isna returns array for arrays, not bool)
    if isinstance(value, np.ndarray):
        if value.size == 0:
            return False
        # For scalar arrays (0-d), extract the value for further checks
        if value.ndim == 0:
            try:
                value = value.item()
            except (ValueError, AttributeError):
                return False
        else:
            # Multi-dimensional array stored as cell value
            # For arrays with data, check if all are NaN
            # Use np.isnan which handles arrays properly
            try:
                if np.issubdtype(value.dtype, np.floating):
                    # For numeric arrays, check if all are NaN
                    return not np.isnan(value).all()
                else:
                    # For non-numeric arrays, if not empty, consider filled
                    return True
            except (ValueError, TypeError):
                # If check fails, assume filled if array is not empty
                return True

    # Check for None/NaN (safe for scalar values only, arrays handled above)
    # Use a try-except to handle edge cases
    try:
        # pd.isna() works for scalars
        isna_result = pd.isna(value)
        # Handle case where pd.isna might return array (shouldn't happen after array check)
        if isinstance(isna_result, np.ndarray):
            return not isna_result.all()
        if isna_result:
            return False
    except (ValueError, TypeError):
        # pd.isna might fail for some types, continue with other checks
        pass

    # Check for empty string
    if isinstance(value, str) and value == "":
        return False

    # Check for empty list/array
    if isinstance(value, (list, tuple)) and len(value) == 0:
        return False

    # Check for empty dict
    if isinstance(value, dict) and len(value) == 0:
        return False

    return True


def count_filled(series: pd.Series) -> int:
    """Count how many values in a Series are considered 'filled'."""
    return series.apply(is_filled).sum()


class DataQualityAnalyzer:
    """Analyzes data quality improvements between two versions of a dataset."""

    def __init__(self, config: ComparisonConfig, verbose: bool = False):
        self.config = config
        self.df_v0: Optional[pd.DataFrame] = None
        self.df_v1: Optional[pd.DataFrame] = None
        self.source_stats: dict[str, SourceStats] = {}
        self.column_stats: list[ColumnStats] = []
        self.verbose = verbose

    def load_data(self) -> bool:
        """Load both parquet files."""
        try:
            print(f"Loading {self.config.name} data...")
            self.df_v0 = pd.read_parquet(self.config.v0_path)
            self.df_v1 = pd.read_parquet(self.config.v1_path)
            print(f"  v0: {len(self.df_v0):,} rows, {len(self.df_v0.columns)} columns")
            print(f"  v1: {len(self.df_v1):,} rows, {len(self.df_v1.columns)} columns")
            return True
        except Exception as e:
            print(f"Error loading data: {e}")
            return False

    def _get_v1_col_name(self, v0_col: str) -> Optional[str]:
        """Get the v1 column name for a v0 column (handles mappings)."""
        if v0_col in self.config.column_mappings:
            return self.config.column_mappings[v0_col]
        if v0_col in self.df_v1.columns:
            return v0_col
        return None

    def _get_v0_col_name(self, v1_col: str) -> Optional[str]:
        """Get the v0 column name for a v1 column (reverse lookup)."""
        reverse_mappings = {v: k for k, v in self.config.column_mappings.items()}
        if v1_col in reverse_mappings:
            return reverse_mappings[v1_col]
        if v1_col in self.df_v0.columns:
            return v1_col
        return None

    def _create_composite_key(
        self, df: pd.DataFrame, use_source: bool = True, is_v1: bool = False
    ) -> pd.Series:
        """Create a composite key for row matching.

        For v0: uses (source, id) combination
        For v1: extracts source and original id from id format "{source}--{v0_id}"
        """
        id_col = self.config.id_col
        source_col = self.config.source_col

        if is_v1:
            # In v1, id is in format "{source}--{v0_id}"
            # Extract source and original id to match with v0
            id_series = df[id_col].astype(str)
            # Split on "--" to extract source and original id
            parts = id_series.str.split("--", n=1, expand=True)
            if len(parts.columns) >= 2:
                extracted_source = parts[0]
                original_id = parts[1]
                # Check if any ids don't follow the expected format
                invalid_mask = parts[1].isna()
                if invalid_mask.any():
                    invalid_count = invalid_mask.sum()
                    invalid_ids = id_series[invalid_mask].head(5).tolist()
                    print(
                        f"  ⚠️  Warning: {invalid_count} v1 ids don't follow '{{source}}--{{v0_id}}' format"
                    )
                    if invalid_ids:
                        print(f"     Examples: {invalid_ids}")
                return extracted_source + "||" + original_id.fillna("")
            else:
                # Fallback if format doesn't match (shouldn't happen but be safe)
                print(
                    "  ⚠️  Warning: v1 ids don't follow expected format, using id as-is"
                )
                return id_series

        # For v0, use (source, id) combination
        if use_source and source_col in df.columns:
            return df[source_col].astype(str) + "||" + df[id_col].astype(str)
        return df[id_col].astype(str)

    def analyze_row_changes(self) -> None:
        """Analyze added, removed, and changed rows per source."""
        source_col = self.config.source_col

        # Get all sources from both versions
        sources_v0 = (
            set(self.df_v0[source_col].unique())
            if source_col in self.df_v0.columns
            else set()
        )
        if source_col in self.df_v1.columns:
            sources_v1 = set(self.df_v1[source_col].unique())
        else:
            # Extract sources from v1 ids (format: "{source}--{v0_id}")
            id_series = self.df_v1[self.config.id_col].astype(str)
            parts = id_series.str.split("--", n=1, expand=True)
            sources_v1 = set(parts[0].unique()) if len(parts.columns) > 0 else set()
        all_sources = sources_v0 | sources_v1

        # Create composite keys for matching
        # v0 uses (source, id), v1 id is "{source}--{v0_id}" format
        key_v0 = self._create_composite_key(self.df_v0, use_source=True, is_v1=False)
        key_v1 = self._create_composite_key(self.df_v1, use_source=True, is_v1=True)

        keys_v0_set = set(key_v0)
        keys_v1_set = set(key_v1)

        for source in sorted(all_sources):
            stats = SourceStats(name=source)

            # Filter by source
            mask_v0 = (
                self.df_v0[source_col] == source
                if source_col in self.df_v0.columns
                else pd.Series([True] * len(self.df_v0))
            )
            if source_col in self.df_v1.columns:
                mask_v1 = self.df_v1[source_col] == source
            else:
                # Extract source from v1 id to filter
                id_series = self.df_v1[self.config.id_col].astype(str)
                parts = id_series.str.split("--", n=1, expand=True)
                mask_v1 = (
                    parts[0] == source
                    if len(parts.columns) > 0
                    else pd.Series([False] * len(self.df_v1))
                )

            source_keys_v0 = set(key_v0[mask_v0])
            source_keys_v1 = set(key_v1[mask_v1])

            stats.v0_count = len(source_keys_v0)
            stats.v1_count = len(source_keys_v1)
            stats.added = len(source_keys_v1 - source_keys_v0)
            stats.removed = len(source_keys_v0 - source_keys_v1)
            stats.kept = len(source_keys_v0 & source_keys_v1)

            self.source_stats[source] = stats

        # Also compute global stats
        global_stats = SourceStats(name="TOTAL")
        global_stats.v0_count = len(self.df_v0)
        global_stats.v1_count = len(self.df_v1)
        global_stats.added = len(keys_v1_set - keys_v0_set)
        global_stats.removed = len(keys_v0_set - keys_v1_set)
        global_stats.kept = len(keys_v0_set & keys_v1_set)
        self.source_stats["TOTAL"] = global_stats

    def analyze_column_fill_rates(self) -> None:
        """Analyze fill rates for all columns."""
        # Get all columns from both versions
        v0_cols = set(self.df_v0.columns)
        v1_cols = set(self.df_v1.columns)

        # Track which v1 columns have been matched
        matched_v1_cols = set()

        # First, analyze columns present in v0
        for v0_col in sorted(v0_cols):
            v1_col = self._get_v1_col_name(v0_col)

            stats = ColumnStats(name=v1_col or v0_col)
            stats.v0_name = v0_col if v0_col != v1_col else None

            # v0 stats
            stats.v0_count = count_filled(self.df_v0[v0_col])
            stats.v0_fill_rate = (
                stats.v0_count / len(self.df_v0) * 100 if len(self.df_v0) > 0 else 0
            )

            # v1 stats (if column exists)
            if v1_col and v1_col in self.df_v1.columns:
                stats.v1_count = count_filled(self.df_v1[v1_col])
                stats.v1_fill_rate = (
                    stats.v1_count / len(self.df_v1) * 100 if len(self.df_v1) > 0 else 0
                )
                matched_v1_cols.add(v1_col)

            self.column_stats.append(stats)

        # Add new columns only in v1
        for v1_col in sorted(v1_cols - matched_v1_cols):
            stats = ColumnStats(name=v1_col)
            stats.v1_count = count_filled(self.df_v1[v1_col])
            stats.v1_fill_rate = (
                stats.v1_count / len(self.df_v1) * 100 if len(self.df_v1) > 0 else 0
            )
            self.column_stats.append(stats)

    def get_score_quality_stats(self) -> dict:
        """Get statistics for score_qualite column."""
        score_col = self.config.score_col
        result = {
            "v0_exists": score_col in self.df_v0.columns,
            "v1_exists": score_col in self.df_v1.columns,
        }

        if result["v0_exists"]:
            scores_v0 = pd.to_numeric(self.df_v0[score_col], errors="coerce")
            result["v0_mean"] = scores_v0.mean()
            result["v0_median"] = scores_v0.median()
            result["v0_std"] = scores_v0.std()
            result["v0_min"] = scores_v0.min()
            result["v0_max"] = scores_v0.max()
            result["v0_fill_rate"] = (
                count_filled(self.df_v0[score_col]) / len(self.df_v0) * 100
            )

        if result["v1_exists"]:
            scores_v1 = pd.to_numeric(self.df_v1[score_col], errors="coerce")
            result["v1_mean"] = scores_v1.mean()
            result["v1_median"] = scores_v1.median()
            result["v1_std"] = scores_v1.std()
            result["v1_min"] = scores_v1.min()
            result["v1_max"] = scores_v1.max()
            result["v1_fill_rate"] = (
                count_filled(self.df_v1[score_col]) / len(self.df_v1) * 100
            )

        return result

    def get_cluster_stats(self) -> dict:
        """Get statistics for cluster_id column.

        Handles column name change: cluster_id in v0, _cluster_id in v1.
        """
        cluster_col_v0 = self.config.cluster_col  # Default: cluster_id for v0
        # In v1, the column is renamed to _cluster_id
        cluster_col_v1 = (
            "_cluster_id" if cluster_col_v0 == "cluster_id" else cluster_col_v0
        )

        result = {
            "v0_exists": cluster_col_v0 in self.df_v0.columns,
            "v1_exists": cluster_col_v1 in self.df_v1.columns,
        }

        if result["v0_exists"]:
            clusters_v0 = self.df_v0[cluster_col_v0]
            result["v0_distinct"] = clusters_v0.nunique()
            result["v0_with_cluster"] = count_filled(clusters_v0)
            result["v0_fill_rate"] = result["v0_with_cluster"] / len(self.df_v0) * 100

        if result["v1_exists"]:
            clusters_v1 = self.df_v1[cluster_col_v1]
            result["v1_distinct"] = clusters_v1.nunique()
            result["v1_with_cluster"] = count_filled(clusters_v1)
            result["v1_fill_rate"] = result["v1_with_cluster"] / len(self.df_v1) * 100

        return result

    def _sample_column_values(
        self, col_name: str, df: pd.DataFrame, n: int = 10
    ) -> list:
        """Sample n random filled (non-emptyish) values from a column."""
        if col_name not in df.columns:
            return []

        # Get the series
        series = df[col_name]

        # Filter to filled values (not emptyish)
        filled_mask = series.apply(is_filled)
        filled_series = series[filled_mask]

        if len(filled_series) == 0:
            return []

        # Sample up to n values using pandas sample
        sample_size = min(n, len(filled_series))
        sampled_series = filled_series.sample(n=sample_size, random_state=42)
        sampled_values = sampled_series.tolist()

        # Format values for display (truncate long strings, handle complex types)
        formatted = []
        for val in sampled_values:
            # Check for numpy array first (before pd.isna which returns array for arrays)
            if isinstance(val, np.ndarray):
                if val.size == 0:
                    formatted.append("[empty array]")
                elif val.ndim == 0:
                    # Scalar array, extract value and continue processing
                    try:
                        val = val.item()
                    except (ValueError, AttributeError):
                        formatted.append("[array: scalar]")
                        continue
                else:
                    formatted.append(f"[array: shape {val.shape}]")
                    continue

            # Check for None/NaN (safe for scalar values only, arrays handled above)
            # Note: This shouldn't happen since we filter to filled values, but handle it
            try:
                if pd.isna(val):
                    formatted.append("(null)")
                    continue
            except (ValueError, TypeError):
                # pd.isna might fail for some types, continue with other checks
                pass

            if isinstance(val, str):
                # Truncate long strings
                if len(val) > 50:
                    formatted.append(val[:47] + "...")
                else:
                    formatted.append(val)
            elif isinstance(val, (list, tuple)):
                formatted.append(f"[{len(val)} items]")
            elif isinstance(val, dict):
                formatted.append(f"{{dict: {len(val)} keys}}")
            else:
                formatted.append(str(val))

        return formatted

    def get_global_fill_rate(self) -> dict:
        """Calculate overall fill rate for the entire dataset."""

        def calc_fill_rate(df):
            total_cells = df.size
            filled_cells = sum(count_filled(df[col]) for col in df.columns)
            return filled_cells / total_cells * 100 if total_cells > 0 else 0

        return {
            "v0": calc_fill_rate(self.df_v0),
            "v1": calc_fill_rate(self.df_v1),
        }

    def generate_report(self) -> str:
        """Generate a comprehensive text report."""
        lines = []

        # Header
        lines.append("=" * 80)
        lines.append(f"  DATA QUALITY COMPARISON REPORT: {self.config.name.upper()}")
        lines.append("=" * 80)
        lines.append("")

        # Overview
        lines.append("📊 OVERVIEW")
        lines.append("-" * 40)
        lines.append(
            f"  v0: {len(self.df_v0):>10,} rows | {len(self.df_v0.columns):>3} columns"
        )
        lines.append(
            f"  v1: {len(self.df_v1):>10,} rows | {len(self.df_v1.columns):>3} columns"
        )
        row_diff = len(self.df_v1) - len(self.df_v0)
        row_pct = (row_diff / len(self.df_v0) * 100) if len(self.df_v0) > 0 else 0
        sign = "+" if row_diff >= 0 else ""
        lines.append(f"  Δ:  {sign}{row_diff:>10,} rows ({sign}{row_pct:.1f}%)")
        lines.append("")

        # Global fill rate
        global_fill = self.get_global_fill_rate()
        lines.append("📈 GLOBAL FILL RATE (% of non-null cells)")
        lines.append("-" * 40)
        lines.append(f"  v0: {global_fill['v0']:>6.2f}%")
        lines.append(f"  v1: {global_fill['v1']:>6.2f}%")
        improvement = global_fill["v1"] - global_fill["v0"]
        sign = "+" if improvement >= 0 else ""
        emoji = "✅" if improvement > 0 else ("⚠️" if improvement < 0 else "➖")
        lines.append(f"  {emoji} Improvement: {sign}{improvement:.2f} points")
        lines.append("")

        # Row changes per source
        lines.append("🔄 ROW CHANGES BY SOURCE")
        lines.append("-" * 40)
        lines.append(
            f"  {'Source':<20} {'v0':>8} {'v1':>8} {'Added':>8} {'Removed':>8} {'Kept':>8}"
        )
        lines.append("  " + "-" * 70)

        for source_name in sorted(self.source_stats.keys()):
            if source_name == "TOTAL":
                continue
            stats = self.source_stats[source_name]
            lines.append(
                f"  {stats.name:<20} {stats.v0_count:>8,} {stats.v1_count:>8,} "
                f"{'+' + str(stats.added):>8} {'-' + str(stats.removed):>8} {stats.kept:>8,}"
            )

        # Total line
        total = self.source_stats.get("TOTAL")
        if total:
            lines.append("  " + "-" * 70)
            lines.append(
                f"  {'TOTAL':<20} {total.v0_count:>8,} {total.v1_count:>8,} "
                f"{'+' + str(total.added):>8} {'-' + str(total.removed):>8} {total.kept:>8,}"
            )
        lines.append("")

        # Score quality stats
        score_stats = self.get_score_quality_stats()
        lines.append("⭐ SCORE QUALITÉ ANALYSIS")
        lines.append("-" * 40)
        if score_stats.get("v0_exists") and score_stats.get("v1_exists"):
            lines.append(f"  {'Metric':<15} {'v0':>12} {'v1':>12} {'Change':>12}")
            lines.append("  " + "-" * 55)

            mean_diff = score_stats["v1_mean"] - score_stats["v0_mean"]
            sign = "+" if mean_diff >= 0 else ""
            lines.append(
                f"  {'Mean':<15} {score_stats['v0_mean']:>12.2f} {score_stats['v1_mean']:>12.2f} {sign}{mean_diff:>11.2f}"
            )

            median_diff = score_stats["v1_median"] - score_stats["v0_median"]
            sign = "+" if median_diff >= 0 else ""
            lines.append(
                f"  {'Median':<15} {score_stats['v0_median']:>12.2f} {score_stats['v1_median']:>12.2f} {sign}{median_diff:>11.2f}"
            )

            lines.append(
                f"  {'Std Dev':<15} {score_stats['v0_std']:>12.2f} {score_stats['v1_std']:>12.2f}"
            )
            lines.append(
                f"  {'Min':<15} {score_stats['v0_min']:>12.2f} {score_stats['v1_min']:>12.2f}"
            )
            lines.append(
                f"  {'Max':<15} {score_stats['v0_max']:>12.2f} {score_stats['v1_max']:>12.2f}"
            )

            fill_diff = score_stats["v1_fill_rate"] - score_stats["v0_fill_rate"]
            sign = "+" if fill_diff >= 0 else ""
            lines.append(
                f"  {'Fill Rate %':<15} {score_stats['v0_fill_rate']:>11.1f}% {score_stats['v1_fill_rate']:>11.1f}% {sign}{fill_diff:>10.1f}%"
            )
        elif score_stats.get("v1_exists"):
            lines.append(
                f"  ✨ NEW in v1: mean={score_stats['v1_mean']:.2f}, fill={score_stats['v1_fill_rate']:.1f}%"
            )
        else:
            lines.append("  ⚠️ Column not found in either version")
        lines.append("")

        # Cluster stats
        cluster_stats = self.get_cluster_stats()
        lines.append("🔗 CLUSTER_ID ANALYSIS")
        lines.append("-" * 40)
        if cluster_stats.get("v0_exists") and cluster_stats.get("v1_exists"):
            lines.append(f"  {'Metric':<25} {'v0':>12} {'v1':>12}")
            lines.append("  " + "-" * 55)
            lines.append(
                f"  {'Distinct clusters':<25} {cluster_stats['v0_distinct']:>12,} {cluster_stats['v1_distinct']:>12,}"
            )
            lines.append(
                f"  {'Rows with cluster':<25} {cluster_stats['v0_with_cluster']:>12,} {cluster_stats['v1_with_cluster']:>12,}"
            )
            lines.append(
                f"  {'Fill rate':<25} {cluster_stats['v0_fill_rate']:>11.1f}% {cluster_stats['v1_fill_rate']:>11.1f}%"
            )
        elif cluster_stats.get("v1_exists"):
            lines.append(
                f"  ✨ NEW in v1: {cluster_stats['v1_distinct']:,} distinct clusters, {cluster_stats['v1_fill_rate']:.1f}% coverage"
            )
        else:
            lines.append("  ⚠️ Column not found in either version")
        lines.append("")

        # Column fill rates
        lines.append("📋 COLUMN FILL RATES (sorted by improvement)")
        lines.append("-" * 40)

        # Sort by improvement (descending)
        sorted_cols = sorted(
            self.column_stats, key=lambda x: x.improvement, reverse=True
        )

        lines.append(f"  {'Column':<45} {'v0 %':>8} {'v1 %':>8} {'Change':>12}")
        lines.append("  " + "-" * 75)

        for col_stat in sorted_cols:
            # Display renamed columns clearly
            if col_stat.v0_name and col_stat.v0_name != col_stat.name:
                name = f"{col_stat.v0_name} → {col_stat.name}"
                status = "RENAMED"
            else:
                name = col_stat.name
                status = None

            # Truncate if too long but keep it readable
            if len(name) > 43:
                name = name[:40] + "..."

            v0_str = (
                f"{col_stat.v0_fill_rate:.1f}" if col_stat.v0_fill_rate > 0 else "—"
            )
            v1_str = (
                f"{col_stat.v1_fill_rate:.1f}" if col_stat.v1_fill_rate > 0 else "—"
            )

            if col_stat.v0_fill_rate > 0 and col_stat.v1_fill_rate > 0:
                sign = "+" if col_stat.improvement >= 0 else ""
                change_str = f"{sign}{col_stat.improvement:.1f}"
                if col_stat.improvement > 5:
                    change_str = f"✅ {change_str}"
                elif col_stat.improvement < -5:
                    change_str = f"⚠️ {change_str}"
                if status == "RENAMED":
                    change_str = f"🔄 {change_str}"
            elif col_stat.v1_fill_rate > 0:
                change_str = "✨ NEW"
            else:
                change_str = "🗑️ REMOVED"

            lines.append(f"  {name:<45} {v0_str:>8} {v1_str:>8} {change_str:>12}")

        lines.append("")

        # Verbose: Sample values for each column
        if self.verbose:
            lines.append("🔍 COLUMN VALUE SAMPLES (10 random filled values)")
            lines.append("-" * 80)
            lines.append("")

            # Use the same sorted columns as above
            for col_stat in sorted_cols:
                v0_col = col_stat.v0_name if col_stat.v0_name else col_stat.name
                v1_col = col_stat.name

                # Display column name
                if col_stat.v0_name and col_stat.v0_name != col_stat.name:
                    display_name = f"{col_stat.v0_name} → {col_stat.name}"
                else:
                    display_name = col_stat.name

                lines.append(f"  📌 {display_name}")
                lines.append("  " + "-" * 78)

                # Sample v0 values
                v0_samples = []
                if v0_col and v0_col in self.df_v0.columns:
                    v0_samples = self._sample_column_values(v0_col, self.df_v0, n=10)
                else:
                    v0_samples = ["(column not in v0)"]

                # Sample v1 values
                v1_samples = []
                if v1_col and v1_col in self.df_v1.columns:
                    v1_samples = self._sample_column_values(v1_col, self.df_v1, n=10)
                else:
                    v1_samples = ["(column not in v1)"]

                # Display samples side by side
                max_samples = max(len(v0_samples), len(v1_samples))
                lines.append(f"    {'v0':<38} | {'v1':<38}")
                lines.append("    " + "-" * 38 + " | " + "-" * 38)

                for i in range(max_samples):
                    v0_val = v0_samples[i] if i < len(v0_samples) else ""
                    v1_val = v1_samples[i] if i < len(v1_samples) else ""
                    # Truncate if needed for display
                    v0_display = v0_val[:37] + "..." if len(v0_val) > 40 else v0_val
                    v1_display = v1_val[:37] + "..." if len(v1_val) > 40 else v1_val
                    lines.append(f"    {v0_display:<38} | {v1_display:<38}")

                lines.append("")

        # Summary
        lines.append("=" * 80)
        lines.append("  SUMMARY")
        lines.append("=" * 80)

        # Count improvements
        improved_cols = sum(
            1
            for c in self.column_stats
            if c.improvement > 1 and c.v0_fill_rate > 0 and c.v1_fill_rate > 0
        )
        degraded_cols = sum(
            1
            for c in self.column_stats
            if c.improvement < -1 and c.v0_fill_rate > 0 and c.v1_fill_rate > 0
        )
        new_cols = sum(
            1 for c in self.column_stats if c.v0_fill_rate == 0 and c.v1_fill_rate > 0
        )
        removed_cols = sum(
            1 for c in self.column_stats if c.v0_fill_rate > 0 and c.v1_fill_rate == 0
        )
        renamed_cols = sum(
            1
            for c in self.column_stats
            if c.v0_name and c.v0_name != c.name and c.v1_fill_rate > 0
        )

        lines.append(f"  ✅ Columns with improved fill rate: {improved_cols}")
        lines.append(f"  ⚠️ Columns with degraded fill rate: {degraded_cols}")
        lines.append(f"  🔄 Columns renamed (v0 → v1): {renamed_cols}")
        lines.append(f"  ✨ New columns in v1: {new_cols}")
        lines.append(f"  🗑️ Columns removed in v1: {removed_cols}")
        lines.append("")

        if total:
            net_rows = total.v1_count - total.v0_count
            sign = "+" if net_rows >= 0 else ""
            lines.append(
                f"  📊 Net row change: {sign}{net_rows:,} ({sign}{net_rows / total.v0_count * 100:.1f}%)"
                if total.v0_count > 0
                else f"  📊 Net row change: {sign}{net_rows:,}"
            )
            lines.append(f"  🔄 Rows matched between versions: {total.kept:,}")

        fill_improvement = global_fill["v1"] - global_fill["v0"]
        if fill_improvement > 0:
            lines.append(
                f"  📈 Overall fill rate improved by {fill_improvement:.2f} percentage points"
            )
        elif fill_improvement < 0:
            lines.append(
                f"  📉 Overall fill rate decreased by {abs(fill_improvement):.2f} percentage points"
            )

        lines.append("")
        lines.append("=" * 80)

        return "\n".join(lines)

    def run_analysis(self) -> str:
        """Run the complete analysis and return the report."""
        if not self.load_data():
            return "Failed to load data."

        print("Analyzing row changes...")
        self.analyze_row_changes()

        print("Analyzing column fill rates...")
        self.analyze_column_fill_rates()

        print("Generating report...")
        return self.generate_report()


def analyze_by_source_detail(
    df_v0: pd.DataFrame,
    df_v1: pd.DataFrame,
    source_col: str = "source",
    score_col: str = "score_qualite",
) -> str:
    """Generate detailed per-source statistics."""
    lines = []
    lines.append("\n" + "=" * 80)
    lines.append("  DETAILED PER-SOURCE ANALYSIS")
    lines.append("=" * 80 + "\n")

    all_sources = sorted(
        set(df_v0[source_col].unique()) | set(df_v1[source_col].unique())
    )

    for source in all_sources:
        df_v0_src = df_v0[df_v0[source_col] == source]
        df_v1_src = df_v1[df_v1[source_col] == source]

        lines.append(f"📁 SOURCE: {source}")
        lines.append("-" * 60)
        lines.append(f"  Rows: {len(df_v0_src):,} → {len(df_v1_src):,}")

        # Fill rate per source
        if len(df_v0_src) > 0 and len(df_v1_src) > 0:
            fill_v0 = (
                sum(count_filled(df_v0_src[col]) for col in df_v0_src.columns)
                / df_v0_src.size
                * 100
            )
            fill_v1 = (
                sum(count_filled(df_v1_src[col]) for col in df_v1_src.columns)
                / df_v1_src.size
                * 100
            )
            diff = fill_v1 - fill_v0
            sign = "+" if diff >= 0 else ""
            lines.append(
                f"  Fill rate: {fill_v0:.1f}% → {fill_v1:.1f}% ({sign}{diff:.1f}%)"
            )

        # Score quality per source
        if score_col in df_v0_src.columns and score_col in df_v1_src.columns:
            score_v0 = pd.to_numeric(df_v0_src[score_col], errors="coerce").mean()
            score_v1 = pd.to_numeric(df_v1_src[score_col], errors="coerce").mean()
            if not pd.isna(score_v0) and not pd.isna(score_v1):
                diff = score_v1 - score_v0
                sign = "+" if diff >= 0 else ""
                lines.append(
                    f"  Avg score_qualite: {score_v0:.2f} → {score_v1:.2f} ({sign}{diff:.2f})"
                )

        lines.append("")

    return "\n".join(lines)


def suggest_column_mappings(v0_path: Path, v1_path: Path, name: str = "Table") -> dict:
    """
    Suggest potential column mappings based on name similarity.
    Uses simple heuristics: common prefix, Levenshtein-like similarity.
    """
    from difflib import SequenceMatcher

    df_v0 = pd.read_parquet(v0_path)
    df_v1 = pd.read_parquet(v1_path)

    v0_cols = set(df_v0.columns)
    v1_cols = set(df_v1.columns)

    # Columns only in v0 (potentially renamed or removed)
    only_v0 = v0_cols - v1_cols
    # Columns only in v1 (potentially new or renamed from v0)
    only_v1 = v1_cols - v0_cols

    print(f"\n{'=' * 70}")
    print(f"  COLUMN MAPPING SUGGESTIONS: {name.upper()}")
    print(f"{'=' * 70}")
    print(f"\nColumns only in v0: {len(only_v0)}")
    print(f"Columns only in v1: {len(only_v1)}")
    print(f"Columns in both: {len(v0_cols & v1_cols)}")

    suggestions = {}

    print(f"\n{'─' * 70}")
    print("  POTENTIAL MAPPINGS (v0 → v1)")
    print(f"{'─' * 70}")
    print(f"  {'v0 column':<35} {'v1 candidate':<25} {'similarity':>8}")
    print(f"  {'-' * 68}")

    for v0_col in sorted(only_v0):
        best_match = None
        best_score = 0

        for v1_col in only_v1:
            # Calculate similarity
            score = SequenceMatcher(None, v0_col.lower(), v1_col.lower()).ratio()

            # Boost score if they share a common prefix
            common_prefix_len = 0
            for c1, c2 in zip(v0_col.lower(), v1_col.lower()):
                if c1 == c2:
                    common_prefix_len += 1
                else:
                    break
            if common_prefix_len >= 4:
                score += 0.2

            # Boost if one is substring of the other
            if v0_col.lower() in v1_col.lower() or v1_col.lower() in v0_col.lower():
                score += 0.3

            if score > best_score and score > 0.4:
                best_score = score
                best_match = v1_col

        if best_match:
            suggestions[v0_col] = best_match
            score_str = f"{best_score:.0%}"
            print(f"  {v0_col:<35} {best_match:<25} {score_str:>8}")
        else:
            print(f"  {v0_col:<35} {'(no match found)':<25} {'—':>8}")

    print(f"\n{'─' * 70}")
    print("  NEW COLUMNS IN V1 (no v0 equivalent found)")
    print(f"{'─' * 70}")

    unmatched_v1 = only_v1 - set(suggestions.values())
    for col in sorted(unmatched_v1):
        fill_rate = count_filled(df_v1[col]) / len(df_v1) * 100
        print(f"  {col:<45} fill: {fill_rate:>5.1f}%")

    # Output JSON format for easy copy-paste
    print(f"\n{'─' * 70}")
    print("  SUGGESTED MAPPINGS (JSON format - edit as needed)")
    print(f"{'─' * 70}")

    import json

    # Filter to only high-confidence matches
    high_conf = {
        k: v
        for k, v in suggestions.items()
        if SequenceMatcher(None, k.lower(), v.lower()).ratio() > 0.5
    }
    print(json.dumps(high_conf, indent=4, ensure_ascii=False))

    return suggestions


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Compare v0 and v1 versions of parquet data files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python compare_versions.py --structures-v0 structures_v0.parquet --structures-v1 structures_v1.parquet
  python compare_versions.py --all-files structures_v0.parquet structures_v1.parquet services_v0.parquet services_v1.parquet
  python compare_versions.py --dir ./data/  # Auto-detect files in directory

  # With column mappings:
  python compare_versions.py --structures-v0 s_v0.parquet --structures-v1 s_v1.parquet \\
      --structures-mappings structures_mappings.json

  # Suggest mappings:
  python compare_versions.py --suggest-mappings --structures-v0 s_v0.parquet --structures-v1 s_v1.parquet
        """,
    )

    parser.add_argument(
        "--structures-v0", type=Path, help="Path to structures v0 parquet file"
    )
    parser.add_argument(
        "--structures-v1", type=Path, help="Path to structures v1 parquet file"
    )
    parser.add_argument(
        "--services-v0", type=Path, help="Path to services v0 parquet file"
    )
    parser.add_argument(
        "--services-v1", type=Path, help="Path to services v1 parquet file"
    )
    parser.add_argument(
        "--structures-mappings",
        type=Path,
        help="JSON file with column mappings for structures (v0_col -> v1_col)",
    )
    parser.add_argument(
        "--services-mappings",
        type=Path,
        help="JSON file with column mappings for services (v0_col -> v1_col)",
    )
    parser.add_argument(
        "--dir", type=Path, help="Directory containing parquet files (auto-detect)"
    )
    parser.add_argument(
        "--output", type=Path, help="Output file for report (default: stdout)"
    )
    parser.add_argument(
        "--detailed", action="store_true", help="Include detailed per-source analysis"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Include sample values (10 random filled values) for each column",
    )
    parser.add_argument(
        "--suggest-mappings",
        action="store_true",
        help="Suggest potential column mappings based on name similarity",
    )
    parser.add_argument(
        "--id-col", default="id", help="Name of the ID column (default: id)"
    )
    parser.add_argument(
        "--source-col",
        default="source",
        help="Name of the source column (default: source)",
    )
    parser.add_argument(
        "--score-col",
        default="score_qualite",
        help="Name of the score column (default: score_qualite)",
    )
    parser.add_argument(
        "--cluster-col",
        default="cluster_id",
        help="Name of the cluster column in v0 (default: cluster_id, v1 uses _cluster_id)",
    )

    args = parser.parse_args()

    # Auto-detect files if directory is provided
    if args.dir:
        dir_path = args.dir
        if not dir_path.exists():
            print(f"Error: Directory {dir_path} does not exist")
            sys.exit(1)

        # Try to find files
        for pattern, attr in [
            ("structures_v0*.parquet", "structures_v0"),
            ("structures_v1*.parquet", "structures_v1"),
            ("services_v0*.parquet", "services_v0"),
            ("services_v1*.parquet", "services_v1"),
        ]:
            matches = list(dir_path.glob(pattern))
            if matches and not getattr(args, attr.replace("-", "_"), None):
                setattr(args, attr.replace("-", "_"), matches[0])

    reports = []

    # If suggest-mappings mode, just show suggestions and exit
    if args.suggest_mappings:
        if args.structures_v0 and args.structures_v1:
            suggest_column_mappings(
                args.structures_v0, args.structures_v1, "Structures"
            )
        if args.services_v0 and args.services_v1:
            suggest_column_mappings(args.services_v0, args.services_v1, "Services")
        return

    # Analyze structures
    if args.structures_v0 and args.structures_v1:
        # Load column mappings if provided
        structures_mappings = {}
        if args.structures_mappings and args.structures_mappings.exists():
            import json

            try:
                with open(args.structures_mappings) as f:
                    structures_mappings = json.load(f)
                print(
                    f"Loaded {len(structures_mappings)} column mappings for structures"
                )
            except json.JSONDecodeError as e:
                print(
                    f"Error: Invalid JSON in {args.structures_mappings}: {e}",
                    file=sys.stderr,
                )
                sys.exit(1)
            except Exception as e:
                print(
                    f"Error: Failed to load {args.structures_mappings}: {e}",
                    file=sys.stderr,
                )
                sys.exit(1)

        config = ComparisonConfig(
            name="Structures",
            v0_path=args.structures_v0,
            v1_path=args.structures_v1,
            id_col=args.id_col,
            source_col=args.source_col,
            score_col=args.score_col,
            cluster_col=args.cluster_col,
            column_mappings=structures_mappings,
        )
        analyzer = DataQualityAnalyzer(config, verbose=args.verbose)
        report = analyzer.run_analysis()
        reports.append(report)

        if args.detailed and analyzer.df_v0 is not None and analyzer.df_v1 is not None:
            reports.append(
                analyze_by_source_detail(
                    analyzer.df_v0, analyzer.df_v1, args.source_col, args.score_col
                )
            )

    # Analyze services
    if args.services_v0 and args.services_v1:
        # Load column mappings if provided
        services_mappings = {}
        if args.services_mappings and args.services_mappings.exists():
            import json

            try:
                with open(args.services_mappings) as f:
                    services_mappings = json.load(f)
                print(f"Loaded {len(services_mappings)} column mappings for services")
            except json.JSONDecodeError as e:
                print(
                    f"Error: Invalid JSON in {args.services_mappings}: {e}",
                    file=sys.stderr,
                )
                sys.exit(1)
            except Exception as e:
                print(
                    f"Error: Failed to load {args.services_mappings}: {e}",
                    file=sys.stderr,
                )
                sys.exit(1)

        config = ComparisonConfig(
            name="Services",
            v0_path=args.services_v0,
            v1_path=args.services_v1,
            id_col=args.id_col,
            source_col=args.source_col,
            score_col=args.score_col,
            cluster_col=args.cluster_col,
            column_mappings=services_mappings,
        )
        analyzer = DataQualityAnalyzer(config, verbose=args.verbose)
        report = analyzer.run_analysis()
        reports.append(report)

        if args.detailed and analyzer.df_v0 is not None and analyzer.df_v1 is not None:
            reports.append(
                analyze_by_source_detail(
                    analyzer.df_v0, analyzer.df_v1, args.source_col, args.score_col
                )
            )

    # Output
    full_report = "\n\n".join(reports)

    if args.output:
        args.output.write_text(full_report)
        print(f"Report written to {args.output}")
    else:
        print(full_report)

    if not reports:
        print("No files to analyze. Use --help for usage information.")
        sys.exit(1)


if __name__ == "__main__":
    main()
