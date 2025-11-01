import pandas as pd
import json
import os

pa_county_code_map = {
    1: 'Adams',
    3: 'Allegheny',
    5: 'Armstrong',
    7: 'Beaver',
    9: 'Bedford',
    11: 'Berks',
    13: 'Blair',
    15: 'Bradford',
    17: 'Bucks',
    19: 'Butler',
    21: 'Cambria',
    23: 'Cameron',
    25: 'Carbon',
    27: 'Centre',
    29: 'Chester',
    31: 'Clarion',
    33: 'Clearfield',
    35: 'Clinton',
    37: 'Columbia',
    39: 'Crawford',
    41: 'Cumberland',
    43: 'Dauphin',
    45: 'Delaware',
    47: 'Elk',
    49: 'Erie',
    51: 'Fayette',
    53: 'Forest',
    55: 'Franklin',
    57: 'Fulton',
    59: 'Greene',
    61: 'Huntingdon',
    63: 'Indiana',
    65: 'Jefferson',
    67: 'Juniata',
    69: 'Lackawanna',
    71: 'Lancaster',
    73: 'Lawrence',
    75: 'Lebanon',
    77: 'Lehigh',
    79: 'Luzerne',
    81: 'Lycoming',
    83: 'McKean',
    85: 'Mercer',
    87: 'Mifflin',
    89: 'Monroe',
    91: 'Montgomery',
    93: 'Montour',
    95: 'Northampton',
    97: 'Northumberland',
    99: 'Perry',
    101: 'Philadelphia',
    103: 'Pike',
    105: 'Potter',
    107: 'Schuylkill',
    109: 'Snyder',
    111: 'Somerset',
    113: 'Sullivan',
    115: 'Susquehanna',
    117: 'Tioga',
    119: 'Union',
    121: 'Venango',
    123: 'Warren',
    125: 'Washington',
    127: 'Wayne',
    129: 'Westmoreland',
    131: 'Wyoming',
    133: 'York'
}

file_path = '../Dataset/dirtyData/PA_DWM_COARSE_WOODY_DEBRIS.csv'
location_json_file = 'countyNameCoords/coords.json'
df_debris = pd.read_csv(file_path, low_memory=False)
df_debris = df_debris.dropna(subset=['VOLCF_AC_UNADJ'])

df_debris = df_debris[df_debris['INVYR'].between(2018, 2025)]

df_locations = pd.read_json(location_json_file, orient='index')
df_locations.index.name = 'CountyName'
df_locations.reset_index(inplace=True)

df_debris['CountyName'] = df_debris['COUNTYCD'].map(pa_county_code_map)

df_final = pd.merge(df_debris, df_locations, on='CountyName', how='left')
df_final = df_final.dropna(subset=['lat', 'long', 'VOLCF_AC_UNADJ'])

df_output = df_final.groupby(['lat', 'long', 'INVYR']).agg({
    'VOLCF_AC_UNADJ': 'sum'
}).reset_index()

df_output.columns = ['lat', 'long', 'year', 'VOLCF_AC_UNADJ']
df_output = df_output.sort_values(['year', 'lat', 'long']).reset_index(drop=True)

output_filename = 'coarse_log_data.csv'
output_dir = '../Dataset/cleanData'
output_file_path = os.path.join(output_dir, output_filename)

df_output.to_csv(output_file_path, index=False)