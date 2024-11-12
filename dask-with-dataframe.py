import dask.dataframe as dd
import pandas as pd
from sqlalchemy import create_engine
import logging
import re

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the Excel file path
file_path = 'Data/FinancialStatement-2024-I-ACES.xlsx'

# Define function to clean text
def clean_text(text, max_length=255):
    """Cleans text by removing unwanted characters and limiting length."""
    text = re.sub(r'[^\w\s]', '', text)  # Remove non-alphanumeric characters
    return text[:max_length]  # Limit length of text

# Define function to read and structure Excel sheets
def excel_to_dataframe(file_path, grup_lk_label, sheet_name):
    """
    Reads data from an Excel file and converts it to a structured DataFrame.

    Parameters:
        file_path (str): Path to the Excel file.
        grup_lk_label (str): Fixed label for the 'grup_lk' column.
        sheet_name (str): Name of the sheet to read.

    Returns:
        pd.DataFrame: Structured DataFrame with standardized columns.
    """
    try:
        # Read the specified sheet, skipping the first row with the title
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=1)

        # Rename columns based on assumed position (adjust if necessary)
        column_mapping = {
            df.columns[0]: 'LaporanDetail',        # First column as 'LaporanDetail'
            df.columns[1]: 'CurrentYearInstant',   # Second column as 'CurrentYearInstant'
            df.columns[2]: 'PriorYearInstant'      # Third column as 'PriorYearInstant'
        }
        df.rename(columns=column_mapping, inplace=True)

        # Add 'grup_lk' column with a constant value
        df['grup_lk'] = grup_lk_label

        # Clean 'LaporanDetail' text
        df['LaporanDetail'] = df['LaporanDetail'].apply(lambda x: clean_text(str(x), max_length=255))

        # Convert numeric columns to numeric types, handling errors gracefully
        df['CurrentYearInstant'] = pd.to_numeric(df['CurrentYearInstant'], errors='coerce').fillna(0)
        df['PriorYearInstant'] = pd.to_numeric(df['PriorYearInstant'], errors='coerce').fillna(0)

        # Return only relevant columns
        return df[['grup_lk', 'LaporanDetail', 'CurrentYearInstant', 'PriorYearInstant']]

    except Exception as e:
        logger.error(f"Failed to process Excel file sheet {sheet_name}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

# Extract the 'Kode Entitas' (emitent) from the "General information" sheet
try:
    general_info_df = pd.read_excel(file_path, sheet_name='1000000', header=None)  # Adjust header as needed
    emitent_value = general_info_df.loc[general_info_df[0] == 'Kode entitas', 1].values[0]
except Exception as e:
    logger.error(f"Failed to extract emitent value: {e}")
    exit(1)

# Read and structure each sheet using excel_to_dataframe
laporan_laba_rugi_pd = excel_to_dataframe(file_path, 'Laba Rugi', '1311000')
laporan_arus_kas_pd = excel_to_dataframe(file_path, 'Arus Kas', '1510000')
laporan_posisi_keuangan_pd = excel_to_dataframe(file_path, 'Posisi Keuangan', '1210000')

# Add an ID column to each DataFrame
laporan_laba_rugi_pd['ID'] = range(1, len(laporan_laba_rugi_pd) + 1)
laporan_arus_kas_pd['ID'] = range(1, len(laporan_arus_kas_pd) + 1)
laporan_posisi_keuangan_pd['ID'] = range(1, len(laporan_posisi_keuangan_pd) + 1)

# Add the 'emitent' column with the extracted value
laporan_laba_rugi_pd['emitent'] = emitent_value
laporan_arus_kas_pd['emitent'] = emitent_value
laporan_posisi_keuangan_pd['emitent'] = emitent_value

# Reorder columns to make sure 'ID' and 'emitent' come first
laporan_laba_rugi_pd = laporan_laba_rugi_pd[['ID', 'emitent', 'grup_lk', 'LaporanDetail', 'CurrentYearInstant', 'PriorYearInstant']]
laporan_arus_kas_pd = laporan_arus_kas_pd[['ID', 'emitent', 'grup_lk', 'LaporanDetail', 'CurrentYearInstant', 'PriorYearInstant']]
laporan_posisi_keuangan_pd = laporan_posisi_keuangan_pd[['ID', 'emitent', 'grup_lk', 'LaporanDetail', 'CurrentYearInstant', 'PriorYearInstant']]

# Concatenate all DataFrames into one
combined_df = pd.concat([laporan_laba_rugi_pd, laporan_arus_kas_pd, laporan_posisi_keuangan_pd])

# Convert the combined DataFrame to Dask DataFrame
combined_dask_df = dd.from_pandas(combined_df, npartitions=1)

# Define the MySQL database connection
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'financial_statement'
}

# Create SQLAlchemy engine
try:
    engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
except Exception as e:
    logger.error(f"Failed to create database engine: {e}")
    exit(1)

# Save the combined Dask DataFrame to MySQL
try:
    combined_dask_df.compute().to_sql('laporan_keuangan', con=engine, if_exists='replace', index=False)
    logger.info("Data berhasil disimpan ke dalam tabel laporan_keuangan di database MySQL.")
except Exception as e:
    logger.error(f"Failed to save DataFrame to MySQL: {e}")
