import pandas as pd
import os

class CreateDataSet:

    def __init__(self, input_file, output_dir, columns):
        self.file_path = input_file
        self.output_dir = output_dir
        self.columns = columns
        
        self.raw_df = None
        self.processed_data = None
        
        os.makedirs(output_dir, exist_ok=True)

    def read_dataset(self):
        try:
            print(f"Reading file: {self.file_path}")
            self.raw_df = pd.read_csv(self.file_path, sep='\t', usecols=self.columns)

        except Exception as e:
            print(f"Failed to read dataset: {e}")
            raise

    def process_data(self):
        if self.raw_df is None:
            self.read_dataset()
        
        df_filtered = self.raw_df[self.raw_df['stateProvince'] == 'Pennsylvania'].copy()
        df_filtered.dropna(subset=['year', 'decimalLatitude', 'decimalLongitude'], inplace=True)
        df_filtered['year'] = df_filtered['year'].astype(int)
        df_filtered['month'] = df_filtered['month'].astype(int)

        df_filtered['latitudeGrid'] = df_filtered['decimalLatitude'].round(1)
        df_filtered['longitudeGrid'] = df_filtered['decimalLongitude'].round(1)

        self.processed_data = df_filtered.groupby(['latitudeGrid', 'longitudeGrid', 'year', 'month']).size().reset_index(name='sightingCount')
        
    def save_data_by_year(self, beginning_year=2018):
        if self.processed_data is None:
            self.process_data()
            
        latest_year = self.processed_data['year'].max()

        for current_year in range(beginning_year, latest_year + 1):
            year_data = self.processed_data[self.processed_data['year'] == current_year]
            
            if not year_data.empty:
                output_filename = f'sightings_by_grid_per_year_{current_year}.csv'
                output_file_path = os.path.join(self.output_dir, output_filename)
                
                year_data.to_csv(output_file_path, index=False)
                print(f'Successfully saved: {output_file_path}')
            else:
                print(f'No data for {current_year}, file not created.')

if __name__ == "__main__":

    INPUT_FILE = '../Dataset/dirtyData/0010762-251025141854904.csv'
    OUTPUT_FOLDER = '../Dataset/cleanData'
    COLUMNS = ['stateProvince', 'year', 'month', 'decimalLatitude', 'decimalLongitude']

    try:
        data_processor = CreateDataSet(input_file=INPUT_FILE, output_dir=OUTPUT_FOLDER, columns=COLUMNS)
        
        data_processor.read_dataset()
        data_processor.process_data()
        data_processor.save_data_by_year(beginning_year=2018)

    except Exception as e:
        print(f"An error occurred in the main script: {e}")