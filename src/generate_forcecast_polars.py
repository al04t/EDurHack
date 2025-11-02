import polars as pl
import numpy as np
from datetime import datetime

def generate_forecast(input_file, output_file, start_year=2018, end_year=2118, noise_level=0.1):
    """
    Generate woodchuck forecast using Polars with random noise for realism.
    
    Args:
        input_file: Path to input CSV with woodchucks_with_wood_volume data
        output_file: Path to output CSV for forecast
        start_year: Start year for forecast (default 2018)
        end_year: End year for forecast (default 2118)
        noise_level: Standard deviation of noise as fraction of value (default 0.1 = 10%)
    """
    
    print(f"Starting forecast generation at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Reading data from: {input_file}")
    
    # Read CSV with Polars (faster than pandas)
    df = pl.read_csv(input_file)
    print(f"Loaded {len(df)} rows from input file")
    
    # Get unique locations
    locations = df.select(['latitude', 'longitude']).unique()
    num_locations = len(locations)
    print(f"Found {num_locations} unique locations")
    
    # Create year range for forecast
    years_to_forecast = np.arange(start_year, end_year + 1)
    num_years = len(years_to_forecast)
    print(f"Generating forecast for {num_years} years ({start_year}-{end_year})")
    print(f"Adding random noise (±{noise_level*100}% standard deviation)")
    
    forecast_data = []
    
    # Set random seed for reproducibility
    np.random.seed(42)
    
    # Process each location
    for idx, row in enumerate(locations.iter_rows(named=True)):
        if (idx + 1) % 100 == 0:
            print(f"Processing location {idx + 1}/{num_locations}...")
        
        lat, lon = row['latitude'], row['longitude']
        
        # Get most recent data for this location
        location_data = df.filter(
            (pl.col('latitude') == lat) & (pl.col('longitude') == lon)
        ).sort('year')
        
        if location_data.height > 0:
            # Get the most recent row
            latest = location_data.tail(1).row(0, named=True)
            
            # Generate base values
            base_population = latest['estimated_woodchuck_population']
            base_volcf = latest['VOLCF_AC_UNADJ']
            base_per_woodchuck = latest['wood_chucked_per_woodchuck_lbs']
            base_total_wood = latest['total_wood_chucked_lbs']
            
            # Add random noise to each value
            population_noise = np.random.normal(1.0, noise_level, num_years)
            volcf_noise = np.random.normal(1.0, noise_level, num_years)
            per_woodchuck_noise = np.random.normal(1.0, noise_level, num_years)
            
            # Apply noise (ensure values stay positive)
            populations = np.maximum(base_population * population_noise, 1)
            volcf_values = np.maximum(base_volcf * volcf_noise, 0.1)
            per_woodchuck_values = np.maximum(base_per_woodchuck * per_woodchuck_noise, 0.1)
            total_wood_values = populations * per_woodchuck_values
            
            # Create forecast dataframe for this location
            location_forecast = pl.DataFrame({
                'year': years_to_forecast,
                'latitude': [lat] * num_years,
                'longitude': [lon] * num_years,
                'estimated_woodchuck_population': populations.tolist(),
                'VOLCF_AC_UNADJ': volcf_values.tolist(),
                'wood_chucked_per_woodchuck_lbs': per_woodchuck_values.tolist(),
                'total_wood_chucked_lbs': total_wood_values.tolist(),
            })
            
            forecast_data.append(location_forecast)
    
    print(f"Concatenating {len(forecast_data)} location forecasts...")
    
    # Concatenate all forecasts
    forecast_df = pl.concat(forecast_data)
    
    # Sort by year (and then by latitude, longitude for consistency)
    print(f"Sorting by year...")
    forecast_df = forecast_df.sort(['year', 'latitude', 'longitude'])
    
    print(f"Writing {len(forecast_df)} forecast rows to: {output_file}")
    forecast_df.write_csv(output_file)
    
    print(f"✓ Forecast generation complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"✓ Total rows generated: {len(forecast_df):,}")
    print(f"✓ Output file: {output_file}")
    
    return forecast_df


def generate_forecast_with_growth(input_file, output_file, start_year=2018, end_year=2118, noise_level=0.1):
    """
    Generate forecast with exponential growth model and random noise based on historical trends.
    
    Args:
        input_file: Path to input CSV with woodchucks_with_wood_volume data
        output_file: Path to output CSV for forecast
        start_year: Start year for forecast (default 2018)
        end_year: End year for forecast (default 2118)
        noise_level: Standard deviation of noise as fraction of value (default 0.1 = 10%)
    """
    
    print(f"Starting forecast generation with growth model at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Reading data from: {input_file}")
    
    df = pl.read_csv(input_file)
    print(f"Loaded {len(df)} rows from input file")
    
    # Get unique locations
    locations = df.select(['latitude', 'longitude']).unique()
    num_locations = len(locations)
    print(f"Found {num_locations} unique locations")
    
    years_to_forecast = np.arange(start_year, end_year + 1)
    num_years = len(years_to_forecast)
    print(f"Generating forecast with growth model for {num_years} years ({start_year}-{end_year})")
    print(f"Adding random noise (±{noise_level*100}% standard deviation)")
    
    forecast_data = []
    
    # Set random seed for reproducibility
    np.random.seed(42)
    
    for idx, row in enumerate(locations.iter_rows(named=True)):
        if (idx + 1) % 100 == 0:
            print(f"Processing location {idx + 1}/{num_locations}...")
        
        lat, lon = row['latitude'], row['longitude']
        
        # Get all data for this location sorted by year
        location_data = df.filter(
            (pl.col('latitude') == lat) & (pl.col('longitude') == lon)
        ).sort('year')
        
        if location_data.height >= 2:
            # Calculate growth rate from historical data
            location_rows = location_data.to_dicts()
            
            first_row = location_rows[0]
            latest_row = location_rows[-1]
            
            base_year = first_row['year']
            final_year = latest_row['year']
            years_span = final_year - base_year
            
            base_population = first_row['estimated_woodchuck_population']
            final_population = latest_row['estimated_woodchuck_population']
            
            # Calculate annual growth rate
            if years_span > 0 and base_population > 0:
                growth_rate = (final_population / base_population) ** (1 / years_span)
            else:
                growth_rate = 1.0
            
            # Project population forward from latest data
            years_from_latest = years_to_forecast - final_year
            projected_populations = final_population * (growth_rate ** years_from_latest)
            
            # Add random noise to populations
            population_noise = np.random.normal(1.0, noise_level, num_years)
            projected_populations = np.maximum(projected_populations * population_noise, 1)
            
            # Add noise to per-woodchuck values
            base_per_woodchuck = latest_row['wood_chucked_per_woodchuck_lbs']
            per_woodchuck_noise = np.random.normal(1.0, noise_level, num_years)
            per_woodchuck_values = np.maximum(base_per_woodchuck * per_woodchuck_noise, 0.1)
            
            # Calculate projected wood chucked
            projected_wood = projected_populations * per_woodchuck_values
            
            # Add noise to VOLCF
            base_volcf = latest_row['VOLCF_AC_UNADJ']
            volcf_noise = np.random.normal(1.0, noise_level, num_years)
            volcf_values = np.maximum(base_volcf * volcf_noise, 0.1)
            
            location_forecast = pl.DataFrame({
                'year': years_to_forecast,
                'latitude': [lat] * num_years,
                'longitude': [lon] * num_years,
                'estimated_woodchuck_population': projected_populations.tolist(),
                'VOLCF_AC_UNADJ': volcf_values.tolist(),
                'wood_chucked_per_woodchuck_lbs': per_woodchuck_values.tolist(),
                'total_wood_chucked_lbs': projected_wood.tolist(),
            })
            
            forecast_data.append(location_forecast)
        elif location_data.height == 1:
            # Single data point, use as-is for all years with noise
            latest = location_data.row(0, named=True)
            
            base_population = latest['estimated_woodchuck_population']
            base_per_woodchuck = latest['wood_chucked_per_woodchuck_lbs']
            base_volcf = latest['VOLCF_AC_UNADJ']
            
            # Add random noise
            population_noise = np.random.normal(1.0, noise_level, num_years)
            populations = np.maximum(base_population * population_noise, 1)
            
            per_woodchuck_noise = np.random.normal(1.0, noise_level, num_years)
            per_woodchuck_values = np.maximum(base_per_woodchuck * per_woodchuck_noise, 0.1)
            
            volcf_noise = np.random.normal(1.0, noise_level, num_years)
            volcf_values = np.maximum(base_volcf * volcf_noise, 0.1)
            
            total_wood_values = populations * per_woodchuck_values
            
            location_forecast = pl.DataFrame({
                'year': years_to_forecast,
                'latitude': [lat] * num_years,
                'longitude': [lon] * num_years,
                'estimated_woodchuck_population': populations.tolist(),
                'VOLCF_AC_UNADJ': volcf_values.tolist(),
                'wood_chucked_per_woodchuck_lbs': per_woodchuck_values.tolist(),
                'total_wood_chucked_lbs': total_wood_values.tolist(),
            })
            
            forecast_data.append(location_forecast)
    
    print(f"Concatenating {len(forecast_data)} location forecasts...")
    forecast_df = pl.concat(forecast_data)
    
    # Sort by year (and then by latitude, longitude for consistency)
    print(f"Sorting by year...")
    forecast_df = forecast_df.sort(['year', 'latitude', 'longitude'])
    
    print(f"Writing {len(forecast_df)} forecast rows to: {output_file}")
    forecast_df.write_csv(output_file)
    
    print(f"✓ Forecast generation complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"✓ Total rows generated: {len(forecast_df):,}")
    print(f"✓ Output file: {output_file}")
    
    return forecast_df


if __name__ == "__main__":
    # Run with growth model (more realistic)
    generate_forecast_with_growth(
        input_file='../Dataset/cleanData/woodchucks_with_wood_volume.csv',
        output_file='../Dataset/cleanData/woodchuck_forecast_hundreds.csv',
        start_year=2018,
        end_year=2118,
        noise_level=0.15  # ±15% noise
    )
    
    # Or run simple forecast (uncomment to use)
    # generate_forecast(
    #     input_file='Dataset/cleanData/woodchucks_with_wood_volume.csv',
    #     output_file='Dataset/cleanData/woodchuck_forecast_hundreds.csv',
    #     start_year=2018,
    #     end_year=2118,
    #     noise_level=0.15
    # )