# ...existing code...
import sys
from pathlib import Path
import pandas as pd

def _find_column(cols, candidates):
    for c in candidates:
        for col in cols:
            if c.lower() in col.lower():
                return col
    return None

def integrate_data(dataset_root: Path = None):
    dataset_root = Path(dataset_root) if dataset_root else Path(__file__).resolve().parent.parent / "Dataset"
    clean_dir = dataset_root / "cleanData"
    pop_file = clean_dir / "populationData.csv"

    sightings_files = sorted(clean_dir.glob("sightings_by_grid_per_year_*.csv"))
    if not sightings_files:
        print("No sightings files found in", clean_dir)
        return

    if not pop_file.exists():
        print("populationData.csv not found in", clean_dir)
        return

    # load and concat all sightings
    dfs = []
    for f in sightings_files:
        df = pd.read_csv(f)
        dfs.append(df)
    sightings = pd.concat(dfs, ignore_index=True)

    # identify key columns
    lat_col = _find_column(sightings.columns, ["latitude", "lat", "latitudeGrid"])
    lon_col = _find_column(sightings.columns, ["longitude", "lon", "longitudeGrid"])
    year_col = _find_column(sightings.columns, ["year"])
    sight_col = _find_column(sightings.columns, ["sighting", "count", "sightingCount", "sightings"])

    if not all([lat_col, lon_col, year_col, sight_col]):
        print("Missing expected columns in sightings. Found:", list(sightings.columns))
        return

    # aggregate yearly counts per grid (in case files contain month-level)
    sightings_agg = sightings.groupby([lat_col, lon_col, year_col], dropna=False)[sight_col].sum().reset_index()

    # load population data
    pop = pd.read_csv(pop_file)
    pop_lat = _find_column(pop.columns, ["latitude", "lat", "latitudeGrid"])
    pop_lon = _find_column(pop.columns, ["longitude", "lon", "longitudeGrid"])
    pop_year = _find_column(pop.columns, ["year"])
    pop_col = _find_column(pop.columns, ["population", "pop", "total"])

    if not pop_col or not pop_lat or not pop_lon:
        print("Population file missing expected columns. Found:", list(pop.columns))
        return

    # choose merge keys
    if pop_year:
        merge_keys = [lat_col, lon_col, year_col]
        # ensure population year column name matches sightings year column name
        pop = pop.rename(columns={pop_lat: lat_col, pop_lon: lon_col, pop_year: year_col, pop_col: "population"})
    else:
        merge_keys = [lat_col, lon_col]
        pop = pop.rename(columns={pop_lat: lat_col, pop_lon: lon_col, pop_col: "population"})

    # normalize column types for merge
    for c in [lat_col, lon_col]:
        sightings_agg[c] = sightings_agg[c].astype(float)
        pop[c] = pop[c].astype(float)

    if year_col in merge_keys:
        sightings_agg[year_col] = sightings_agg[year_col].astype(int)
        pop[year_col] = pop[year_col].astype(int)

    merged = sightings_agg.merge(pop, on=merge_keys, how="left", validate="m:1")

    # compute adjusted metric: sightings per 1000 people
    merged["population"] = pd.to_numeric(merged["population"], errors="coerce")
    merged["sightings_per_1000"] = merged.apply(
        lambda r: (r[sight_col] / r["population"] * 1000) if pd.notna(r["population"]) and r["population"] > 0 else pd.NA,
        axis=1
    )

    out_dir = clean_dir
    # save per year and combined
    years = sorted(merged[year_col].dropna().unique())
    for y in years:
        out = merged[merged[year_col] == int(y)].copy()
        out_path = out_dir / f"adjusted_sightings_by_grid_per_year_{int(y)}.csv"
        out.to_csv(out_path, index=False)
        print("Saved", out_path)

    combined_path = out_dir / "adjusted_sightings_all_years.csv"
    merged.to_csv(combined_path, index=False)
    print("Saved combined file:", combined_path)

if __name__ == "__main__":
    integrate_data()