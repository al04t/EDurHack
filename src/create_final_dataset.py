import pandas as pd
import numpy as np

df_woodchucks = pd.read_csv('../Dataset/cleanData/adjusted_sightings_all_years_minimal.csv')
df_wood = pd.read_csv('../Dataset/cleanData/coarse_log_data.csv')

df_merged = pd.merge(
    df_woodchucks,
    df_wood,
    left_on=['year', 'latitude', 'longitude'],
    right_on=['year', 'lat', 'long'],
    how='left'
)

df_merged = df_merged.drop(columns=['lat', 'long'])

missing_mask = df_merged['VOLCF_AC_UNADJ'].isna()

for idx in df_merged[missing_mask].index:
    year = df_merged.loc[idx, 'year']
    lat = df_merged.loc[idx, 'latitude']
    lon = df_merged.loc[idx, 'longitude']

    year_wood = df_wood[df_wood['year'] == year].copy()
    
    if len(year_wood) > 0:
        year_wood['distance'] = np.sqrt((year_wood['lat'] - lat)**2 + (year_wood['long'] - lon)**2)

        nearest_idx = year_wood['distance'].idxmin()
        df_merged.loc[idx, 'VOLCF_AC_UNADJ'] = year_wood.loc[nearest_idx, 'VOLCF_AC_UNADJ']

df_merged = df_merged.sort_values(['year', 'latitude', 'longitude']).reset_index(drop=True)

min_wood = df_merged['VOLCF_AC_UNADJ'].min()
max_wood = df_merged['VOLCF_AC_UNADJ'].max()

df_merged['wood_chucked_per_woodchuck_lbs'] = (
    (df_merged['VOLCF_AC_UNADJ'] - min_wood) / (max_wood - min_wood) * 1000
)

df_merged['total_wood_chucked_lbs'] = (
    df_merged['wood_chucked_per_woodchuck_lbs'] * df_merged['estimated_woodchuck_population']
)

df_merged = df_merged[df_merged['year'] != 2025]

df_merged.to_csv('../Dataset/cleanData/woodchucks_with_wood_volume.csv', index=False)
