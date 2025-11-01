import sys
from pathlib import Path
import math
import pandas as pd


def _find_column(cols, candidates):
    cols_l = [c.lower() for c in cols]
    for cand in candidates:
        for i, col in enumerate(cols_l):
            if cand.lower() in col:
                return list(cols)[i]
    return None


def integrate_data(dataset_root: Path | str | None = None,
                   detection_rate: float = 0.5,
                   estimation_mode: str = "sightings",
                   woodchuck_density_per_km2: float | None = None,
                   calibration_year: int | None = None,
                   calibration_total: float | None = None,
                   calibration_mode: str = "sightings"):
    """Integrate sightings with population and compute adjusted metrics.

    detection_rate: average number of sightings observed per single woodchuck
        over the time window (e.g. per year). Used to estimate population as
        estimated_population = sightingCount / detection_rate. Default 0.5.
    """
    dataset_root = Path(dataset_root) if dataset_root else Path(__file__).resolve().parent.parent / "Dataset"
    clean_dir = dataset_root / "cleanData"
    pop_file = clean_dir / "population_density_by_coords.csv"

    # if the preferred population file doesn't exist, try to find alternatives
    if not pop_file.exists():
        print(f"Preferred population file not found: {pop_file}")
        # look for plausible population/density files in cleanData and dirtyData
        candidates = []
        candidates += list(clean_dir.glob("*population*.csv"))
        candidates += list(clean_dir.glob("*density*.csv"))
        dirty_dir = dataset_root / "dirtyData"
        if dirty_dir.exists():
            candidates += list(dirty_dir.glob("*population*.csv"))
            candidates += list(dirty_dir.glob("*density*.csv"))

        # de-duplicate while preserving order
        seen = set()
        uniq = []
        for p in candidates:
            s = str(p).lower()
            if s not in seen:
                seen.add(s)
                uniq.append(p)

        if uniq:
            pop_file = uniq[0]
            print(f"Using population file: {pop_file}")
        else:
            available = list(clean_dir.iterdir()) if clean_dir.exists() else []
            print("No population/density CSV found in cleanData or dirtyData.")
            print("Files in cleanData:")
            for f in available:
                print(" -", f.name)
            raise FileNotFoundError(f"No population file found. Expected {clean_dir / 'population_density_by_coords.csv'}")

    sightings_files = sorted(clean_dir.glob("sightings_by_grid_per_year_*.csv"))

    # load and concat sightings
    dfs = []
    for f in sightings_files:
        dfs.append(pd.read_csv(f))
    sightings = pd.concat(dfs, ignore_index=True)

    # detect columns in sightings
    lat_s_col = _find_column(sightings.columns, ["latitudeGrid", "lat", "latitude"])
    lon_s_col = _find_column(sightings.columns, ["longitudeGrid", "long", "longitude"])
    year_col = _find_column(sightings.columns, ["year"])
    month_col = _find_column(sightings.columns, ["month"])
    sight_col = _find_column(sightings.columns, ["sightingCount", "sighting_count", "count", "sightings"])

    if not all([lat_s_col, lon_s_col, sight_col]):
        print("Missing expected columns in sightings. Found:", list(sightings.columns))
        return

    # aggregate sightings to grid/year/month as available
    group_cols = [lat_s_col, lon_s_col]
    if year_col:
        group_cols.append(year_col)
    if month_col:
        group_cols.append(month_col)

    sightings_agg = sightings.groupby(group_cols, dropna=False)[sight_col].sum().reset_index()

    # load population
    # Try to load a population/density file that contains lat/lon and a population column.
    def _valid_pop_df(df: pd.DataFrame):
        lat_c = _find_column(df.columns, ["latitudedecimal", "latitude", "lat", "latitudegrid"])
        lon_c = _find_column(df.columns, ["longitundinaldecimal", "longitudedecimal", "longitude", "lon", "longitudegrid"])
        pop_c = _find_column(df.columns, ["population", "pop", "total", "density", "people"])
        return (lat_c, lon_c, pop_c)

    pop = None
    lat_p_col = lon_p_col = pop_col = None

    # First try the selected pop_file, then try other common candidates
    candidates = [pop_file]
    # common filenames to try
    candidates += [clean_dir / "population_density_people_by_coords.csv",
                   clean_dir / "populationData.csv",
                   clean_dir / "population_density_by_coords.csv",
                   clean_dir / "population_data_woodchucks.csv",
                   dataset_root / "dirtyData" / "Population-Density-Final.csv"]
    # add any CSVs in clean_dir that look relevant
    candidates += list(clean_dir.glob("*.csv"))

    seen = set()
    for p in candidates:
        if p is None:
            continue
        sp = str(p).lower()
        if sp in seen:
            continue
        seen.add(sp)
        if not Path(p).exists():
            continue
        try:
            df_try = pd.read_csv(p)
        except Exception:
            continue
        lat_c, lon_c, pop_c = _valid_pop_df(df_try)
        if lat_c and lon_c and pop_c:
            pop = df_try
            lat_p_col, lon_p_col, pop_col = lat_c, lon_c, pop_c
            pop_file = p
            print(f"Using population file: {pop_file}")
            break

    if pop is None:
        # nothing suitable found; show available files and their columns to help the user
        print("No suitable population/density CSV found. Checked these files (name -> columns):")
        for p in sorted({str(x) for x in candidates if Path(x).exists()}):
            try:
                cols = pd.read_csv(p, nrows=0).columns.tolist()
            except Exception:
                cols = ["<unreadable>"]
            print(f" - {Path(p).name}: {cols}")
        raise FileNotFoundError("No population/density file with latitude, longitude and population/density columns was found.")

    # normalize merge keys: round to 1 decimal to match grid rounding used earlier
    sightings_agg = sightings_agg.copy()
    sightings_agg["lat"] = sightings_agg[lat_s_col].astype(float).round(1)
    sightings_agg["lon"] = sightings_agg[lon_s_col].astype(float).round(1)
    if year_col and year_col in sightings_agg.columns:
        sightings_agg[year_col] = sightings_agg[year_col].astype(int)
        # normalize to a standard 'year' column for outputs
        sightings_agg["year"] = sightings_agg[year_col]
    if month_col and month_col in sightings_agg.columns:
        try:
            sightings_agg[month_col] = sightings_agg[month_col].astype(int)
            sightings_agg["month"] = sightings_agg[month_col]
        except Exception:
            # leave as-is if conversion fails
            sightings_agg["month"] = sightings_agg[month_col]

    pop = pop.copy()
    pop["lat"] = pop[lat_p_col].astype(float).round(1)
    pop["lon"] = pop[lon_p_col].astype(float).round(1)
    pop["population"] = pd.to_numeric(pop[pop_col], errors="coerce")

    # keep only needed pop cols (if there are duplicates, keep first by lat/lon)
    pop_unique = pop.groupby(["lat", "lon"], as_index=False).agg({"population": "sum"})

    # merge
    if year_col:
        merged = sightings_agg.merge(pop_unique, on=["lat", "lon"], how="left", validate="m:1")
    else:
        merged = sightings_agg.merge(pop_unique, on=["lat", "lon"], how="left", validate="m:1")

    # compute adjusted metric: sightings per 1000 people
    def adj(row):
        popv = row.get("population")
        sc = row.get(sight_col)
        if pd.isna(popv) or popv == 0:
            return pd.NA
        return float(sc) / float(popv) * 1000.0

    merged["sightings_per_1000"] = merged.apply(adj, axis=1)

    # expose latitude/longitude (grid center values) in clearer column names
    # these are the 0.1-degree rounded coordinates used for merging
    merged["latitude"] = merged["lat"]
    merged["longitude"] = merged["lon"]

    # Compute grid cell area (approx) in km^2 from 0.1-degree grid centered at latitude
    # 1 degree latitude ≈ 111.32 km. Longitude length scales by cos(latitude).
    # So area ≈ (111.32 * dlon) * (111.32 * dlat * cos(lat_rad)), with dlat=dlon=0.1
    def grid_area_km2(lat_deg: float) -> float:
        try:
            lat_rad = math.radians(float(lat_deg))
            side_lat = 111.32 * 0.1
            side_lon = 111.32 * 0.1 * math.cos(lat_rad)
            return abs(side_lat * side_lon)
        except Exception:
            return float("nan")

    merged["grid_area_km2"] = merged["latitude"].apply(grid_area_km2)

    # Estimate woodchuck population.
    # Modes:
    #  - "sightings": use sightings / detection_rate (default behavior)
    #  - "density": use woodchuck_density_per_km2 * grid_area_km2 (requires density)
    #  - "hybrid": prefer density if provided, otherwise fall back to sightings
    def est_pop(row):
        sc = row.get(sight_col)
        area = row.get("grid_area_km2")

        if estimation_mode == "density":
            if woodchuck_density_per_km2 is None or pd.isna(area):
                return pd.NA
            return float(woodchuck_density_per_km2) * float(area)

        if estimation_mode == "hybrid":
            if woodchuck_density_per_km2 is not None and not pd.isna(area):
                return float(woodchuck_density_per_km2) * float(area)
            # else fallback to sightings

        # default: sightings-based estimate
        try:
            if pd.isna(sc):
                return pd.NA
            if detection_rate is None or detection_rate <= 0:
                return pd.NA
            return float(sc) / float(detection_rate)
        except Exception:
            return pd.NA

    # compute both estimates (sightings-based and density-based) and pick default
    def est_by_sight(row):
        sc = row.get(sight_col)
        try:
            if pd.isna(sc) or detection_rate is None or detection_rate <= 0:
                return pd.NA
            return float(sc) / float(detection_rate)
        except Exception:
            return pd.NA

    def est_by_density(row):
        area = row.get("grid_area_km2")
        if woodchuck_density_per_km2 is None or pd.isna(area):
            return pd.NA
        return float(woodchuck_density_per_km2) * float(area)

    merged["estimated_by_sightings"] = merged.apply(est_by_sight, axis=1)
    merged["estimated_by_density"] = merged.apply(est_by_density, axis=1)

    # pick primary estimate based on estimation_mode
    if estimation_mode == "density":
        merged["estimated_woodchuck_population"] = merged["estimated_by_density"]
    elif estimation_mode == "hybrid":
        # prefer density when available, else sightings
        merged["estimated_woodchuck_population"] = merged.apply(
            lambda r: r["estimated_by_density"] if pd.notna(r["estimated_by_density"]) else r["estimated_by_sightings"],
            axis=1,
        )
    else:
        merged["estimated_woodchuck_population"] = merged["estimated_by_sightings"]

    # Optional calibration: scale estimates so that the chosen calibration_mode
    # sums to calibration_total for calibration_year. This helps match external
    # known totals (e.g., regional surveys) and correct systematic under/over-estimates.
    merged["estimated_woodchuck_population_calibrated"] = pd.NA
    if calibration_year is not None and calibration_total is not None:
        # filter rows for the calibration year (if year column exists)
        if "year" in merged.columns:
            mask = merged["year"] == int(calibration_year)
            subset = merged[mask]
        else:
            subset = merged

        if calibration_mode == "density":
            sum_est = subset["estimated_by_density"].dropna().sum()
        else:
            sum_est = subset["estimated_by_sightings"].dropna().sum()

        if sum_est and sum_est > 0:
            factor = float(calibration_total) / float(sum_est)
            merged["estimated_woodchuck_population_calibrated"] = (merged["estimated_woodchuck_population"].astype(float) * factor)
            print(f"Applied calibration factor {factor:.4f} (target {calibration_total}, sum_est {sum_est:.2f})")
        else:
            print("Calibration requested but sum of estimates is zero or unavailable; skipping calibration.")

    out_dir = clean_dir
    # prepare minimal output: latitude, longitude, estimated woodchuck population
    # include standardized year/month columns if present
    output_cols = ["latitude", "longitude", "estimated_woodchuck_population"]
    if "year" in merged.columns:
        output_cols.insert(0, "year")
    if "month" in merged.columns:
        # place month after year if year exists, otherwise at front
        if "year" in merged.columns:
            output_cols.insert(1, "month")
        else:
            output_cols.insert(0, "month")
    # include calibrated estimate column if it exists
    if "estimated_woodchuck_population_calibrated" in merged.columns and merged["estimated_woodchuck_population_calibrated"].notna().any():
        # place calibrated column right after the raw estimate
        try:
            idx = output_cols.index("estimated_woodchuck_population")
            output_cols.insert(idx + 1, "estimated_woodchuck_population_calibrated")
        except ValueError:
            output_cols.append("estimated_woodchuck_population_calibrated")

    output_df = merged[output_cols].copy()
    # drop rows without an estimated population
    output_df = output_df.dropna(subset=["estimated_woodchuck_population"])

    # save combined minimal file
    combined_path = out_dir / "adjusted_sightings_all_years_minimal.csv"
    output_df.to_csv(combined_path, index=False)
    print("Saved combined minimal adjusted file:", combined_path)

    # save per-year minimal files (if year exists)
    if year_col:
        years = sorted(merged[year_col].dropna().unique())
        for y in years:
            per = merged[merged[year_col] == int(y)][output_cols].copy()
            per = per.dropna(subset=["estimated_woodchuck_population"])
            out_path = out_dir / f"adjusted_sightings_by_grid_per_year_{int(y)}_minimal.csv"
            per.to_csv(out_path, index=False)
            print("Saved", out_path)


if __name__ == "__main__":
    integrate_data()