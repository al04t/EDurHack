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
            self.raw_df = pd.read_csv(self.file_path, 
                                      usecols=self.columns, 
                                      low_memory=False)

        except Exception as e:
            print(f"Failed to read dataset: {e}")
            raise

    def process_data(self):
        if self.raw_df is None:
            self.read_dataset()
        
        df_filtered = self.raw_df[self.raw_df['St'] == 'Pennsylvania'].copy()
        df_filtered['latitudeGrid'] = df_filtered['lat'].round(1)
        df_filtered['longitudeGrid'] = df_filtered['long'].round(1)

        self.processed_data = df_filtered

    def save_data(self):
        output_filename = f'population_density_by_coords.csv'
        output_file_path = os.path.join(self.output_dir, output_filename)

        if self.processed_data is not None and not self.processed_data.empty:
            self.processed_data.to_csv(output_file_path, index=False)
            print(f"Successfully saved data to: {output_file_path}")
        else:
            print("No processed data to save.")


if __name__ == "__main__":

    INPUT_FILE = '../Dataset/dirtyData/Population-Density-Final.csv'
    OUTPUT_FOLDER = '../Dataset/cleanData'
    COLUMNS = ['population', 'density', 'St', 'lat', 'long']

    try:
        data_processor = CreateDataSet(input_file=INPUT_FILE, output_dir=OUTPUT_FOLDER, columns=COLUMNS)
        
        data_processor.read_dataset()
        data_processor.process_data()
        data_processor.save_data()

    except Exception as e:
        print(f"An error occurred in the main script: {e}")