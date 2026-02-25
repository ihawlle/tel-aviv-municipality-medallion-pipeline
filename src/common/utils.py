"""Shared utility helpers for the medallion pipeline."""

from __future__ import annotations

import json
import math

from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StructField, StructType


_TITLE_STOPWORDS = ["הרב", "פרופ", "פרופסור", "דר", "דוקטור", "הנשיא"]


def generate_street_signature(street_col: Column) -> Column:
    """
    Build canonical street signature for resilient matching.

    Rules:
    - Remove quotes/geresh and punctuation consistently.
    - Keep Hebrew words only.
    - Remove common titles/prefixes.
    - Sort words alphabetically and re-join.
    """
    base_text = F.coalesce(street_col.cast("string"), F.lit(""))
    no_quotes = F.regexp_replace(base_text, r"[\"'׳״]", " ")
    hebrew_only = F.regexp_replace(no_quotes, r"[^א-ת0-9\s]", " ")
    normalized = F.trim(F.regexp_replace(hebrew_only, r"\s+", " "))

    words = F.split(normalized, " ")
    stopwords = F.array(*[F.lit(word) for word in _TITLE_STOPWORDS])
    filtered_words = F.filter(
        words,
        lambda w: (w != F.lit("")) & (~F.array_contains(stopwords, w)),
    )
    return F.trim(F.array_join(F.sort_array(filtered_words), " "))


def euclidean_distance_m(
    x1_col: Column,
    y1_col: Column,
    x2_col: Column,
    y2_col: Column,
) -> Column:
    """Compute Euclidean distance in meters for ITM coordinates."""
    return F.sqrt(F.pow(x1_col - x2_col, 2) + F.pow(y1_col - y2_col, 2))


def _parse_paths_from_json(json_str: str | None) -> list | None:
    """
    Parse paths from either a raw paths array string '[[[x,y],...]]'
    or a geometry object string '{"paths": [[[x,y],...]], ...}'.
    Returns paths list or None.
    """
    if not json_str or not json_str.strip():
        return None
    try:
        data = json.loads(json_str)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("paths") or data.get("path") or None
        return None
    except (json.JSONDecodeError, TypeError):
        return None


def _segment_midpoint_from_paths(paths: list | None) -> tuple[float | None, float | None]:
    """Compute midpoint from first path; returns (None, None) if invalid."""
    if not paths or len(paths) == 0:
        return (None, None)
    path0 = paths[0]
    if not path0 or not isinstance(path0, (list, tuple)) or len(path0) < 2:
        return (None, None)
    first, last = path0[0], path0[-1]
    if not first or not last or len(first) < 2 or len(last) < 2:
        return (None, None)
    try:
        x1, y1 = float(first[0]), float(first[1])
        x2, y2 = float(last[0]), float(last[1])
        return ((x1 + x2) / 2.0, (y1 + y2) / 2.0)
    except (TypeError, ValueError, IndexError):
        return (None, None)


@F.udf(StructType([StructField("mid_x", DoubleType()), StructField("mid_y", DoubleType())]))
def segment_midpoint_from_paths_json(paths_json: str | None) -> tuple[float | None, float | None] | None:
    """
    Extract segment midpoint (x, y) from ArcGIS paths JSON.
    Accepts either paths array '[[[x,y],[x,y],...]]' or geometry '{"paths": [...]}'.
    Returns (mid_x, mid_y) or (None, None) if parsing fails.
    """
    if paths_json is not None and not isinstance(paths_json, str):
        paths_json = str(paths_json)
    paths = _parse_paths_from_json(paths_json)
    return _segment_midpoint_from_paths(paths)


@F.udf(ArrayType(ArrayType(ArrayType(DoubleType()))))
def parse_paths_array_json(paths_json: str | None):
    """
    Parse paths JSON string to array of paths for point_to_polyline_distance_m.
    Accepts paths array '[[[x,y],...]]' or geometry '{"paths": [...]}'.
    Returns list of paths or null.
    """
    if paths_json is not None and not isinstance(paths_json, str):
        paths_json = str(paths_json)
    paths = _parse_paths_from_json(paths_json)
    if not paths:
        return None
    result = []
    for path in paths:
        if not path or not isinstance(path, (list, tuple)):
            continue
        coords = []
        for pt in path:
            if isinstance(pt, (list, tuple)) and len(pt) >= 2:
                try:
                    coords.append([float(pt[0]), float(pt[1])])
                except (TypeError, ValueError):
                    pass
        if coords:
            result.append(coords)
    return result if result else None


def _point_to_segment_distance(px: float, py: float, x1: float, y1: float, x2: float, y2: float) -> float:
    """Compute point-to-line-segment distance in Cartesian coordinates."""
    dx = x2 - x1
    dy = y2 - y1
    if dx == 0 and dy == 0:
        return math.hypot(px - x1, py - y1)

    t = ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)
    t = max(0.0, min(1.0, t))
    proj_x = x1 + t * dx
    proj_y = y1 + t * dy
    return math.hypot(px - proj_x, py - proj_y)


@F.udf(DoubleType())
def point_to_polyline_distance_m(
    x: float | None,
    y: float | None,
    paths: list[list[list[float]]] | None,
) -> float | None:
    """
    Compute nearest distance between a point and polyline paths in ITM meters.

    paths format: [[[x1,y1],[x2,y2],...], [...]]
    """
    if x is None or y is None or not paths:
        return None

    best: float | None = None
    for path in paths:
        if not path or len(path) < 2:
            continue
        for i in range(len(path) - 1):
            p1 = path[i]
            p2 = path[i + 1]
            if len(p1) < 2 or len(p2) < 2:
                continue
            d = _point_to_segment_distance(float(x), float(y), float(p1[0]), float(p1[1]), float(p2[0]), float(p2[1]))
            if best is None or d < best:
                best = d
    return best
