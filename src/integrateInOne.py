import sys
from pathlib import Path
import pandas as pd


def _find_column(cols, candidates):
    cols_l = [c.lower() for c in cols]
    for cand in candidates:
        for i, col in enumerate(cols_l):
            if cand.lower() in col:
                return list(cols)[i]
    return None


def integrate_data(dataset_root: Path | str | None = None):
    dataset_root = Path(dataset_root) if dataset_root else Path(__file__).resolve().parent.parent / "Dataset"
    clean_dir = dataset_root / "cleanData"
    pop_file = clean_dir / "populationDataPeople.csv"

    sightings_files = sorted(clean_dir.glob("sightings_by_grid_per_year_*.csv"))
    if not sightings_files:
        print("No sightings files found in", clean_dir)
        return

    if not pop_file.exists():
        print("Population file not found:", pop_file)
        return

    # load and concat sightings
    dfs = []
    for f in sightings_files:
        dfs.append(pd.read_csv(f))
    sightings = pd.concat(dfs, ignore_index=True)

    # detect columns in sightings
    lat_s_col = _find_column(sightings.columns, ["latitudeGrid", "lat", "latitude"])
    lon_s_col = _find_column(sightings.columns, ["longitudeGrid", "lon", "longitude"])
    year_col = _find_column(sightings.columns, ["year"])
    sight_col = _find_column(sightings.columns, ["sightingCount", "sighting_count", "count", "sightings"])

    if not all([lat_s_col, lon_s_col, sight_col]):
        print("Missing expected columns in sightings. Found:", list(sightings.columns))
        return

    # aggregate sightings to yearly-per-grid if needed
    if year_col:
        sightings_agg = sightings.groupby([lat_s_col, lon_s_col, year_col], dropna=False)[sight_col].sum().reset_index()
    else:
        sightings_agg = sightings.groupby([lat_s_col, lon_s_col], dropna=False)[sight_col].sum().reset_index()
        year_col = None

    # load population
    pop = pd.read_csv(pop_file)
    # population columns expected: latitudeDecimal, longitundinalDecimal (or longitudeDecimal), density, population
    lat_p_col = _find_column(pop.columns, ["latitudedecimal", "latitude", "lat"])
    lon_p_col = _find_column(pop.columns, ["longitundinaldecimal", "longitudedecimal", "longitude", "lon"])
    pop_col = _find_column(pop.columns, ["population", "pop", "total"])
    if not all([lat_p_col, lon_p_col, pop_col]):
        print("Missing expected columns in population file. Found:", list(pop.columns))
        return

    # normalize merge keys: round to 1 decimal to match grid rounding used earlier
    sightings_agg = sightings_agg.copy()
    sightings_agg["lat"] = sightings_agg[lat_s_col].astype(float).round(1)
    sightings_agg["lon"] = sightings_agg[lon_s_col].astype(float).round(1)
    if year_col:
        sightings_agg[year_col] = sightings_agg[year_col].astype(int)

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

    out_dir = clean_dir
    # save combined file
    combined_path = out_dir / "adjusted_sightings_all_years.csv"
    merged.to_csv(combined_path, index=False)
    print("Saved combined adjusted file:", combined_path)

    # save per-year if year exists
    if year_col:
        years = sorted(merged[year_col].dropna().unique())
        for y in years:
            out = merged[merged[year_col] == int(y)].copy()
            out_path = out_dir / f"adjusted_sightings_by_grid_per_year_{int(y)}.csv"
            out.to_csv(out_path, index=False)
            print("Saved", out_path)


if __name__ == "__main__":
    integrate_data()