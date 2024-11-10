import dask.dataframe as dd
import pandas as pd
from sqlalchemy import create_engine
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load the Excel file
file_path = 'Data/FinancialStatement-2024-I-ACES.xlsx'

# Extract the 'Kode Entitas' (emitent) from the "General information" sheet
try:
    general_info_df = pd.read_excel(file_path, sheet_name='1000000', header=None)  # Adjust header as needed
    emitent_value = general_info_df.loc[general_info_df[0] == 'Kode entitas', 1].values[0]
except Exception as e:
    logger.error(f"Failed to extract emitent value: {e}")
    exit(1)

# Read the sheets into Pandas DataFrames
try:
    laporan_laba_rugi_pd = pd.read_excel(file_path, sheet_name='1311000', header=1)  # Use header=1 to skip unnecessary rows
    laporan_arus_kas_pd = pd.read_excel(file_path, sheet_name='1510000', header=1)
    laporan_posisi_keuangan_pd = pd.read_excel(file_path, sheet_name='1210000', header=1)
except Exception as e:
    logger.error(f"Failed to read Excel file: {e}")
    exit(1)

# Function to truncate or rename column names to avoid MySQL identifier length issue
def truncate_column_names(df, max_length=64):
    df.columns = [col[:max_length] if len(col) > max_length else col for col in df.columns]
    return df

# Truncate column names for each DataFrame
laporan_laba_rugi_pd = truncate_column_names(laporan_laba_rugi_pd)
laporan_arus_kas_pd = truncate_column_names(laporan_arus_kas_pd)
laporan_posisi_keuangan_pd = truncate_column_names(laporan_posisi_keuangan_pd)

# Optionally drop unnecessary columns (add column names to drop as needed)
laporan_laba_rugi_pd.drop(columns=['Unnamed: 3'], inplace=True)
laporan_arus_kas_pd.drop(columns=['Unnamed: 3'], inplace=True)
laporan_posisi_keuangan_pd.drop(columns=['Unnamed: 3'], inplace=True)

# Add an ID column to each DataFrame
laporan_laba_rugi_pd['ID'] = range(1, len(laporan_laba_rugi_pd) + 1)
laporan_arus_kas_pd['ID'] = range(1, len(laporan_arus_kas_pd) + 1)
laporan_posisi_keuangan_pd['ID'] = range(1, len(laporan_posisi_keuangan_pd) + 1)

# Add the 'emitent' column to each DataFrame with the extracted value
laporan_laba_rugi_pd['emitent'] = emitent_value
laporan_arus_kas_pd['emitent'] = emitent_value
laporan_posisi_keuangan_pd['emitent'] = emitent_value

# Add 'LaporanKeuangan' column to identify the type of financial report
laporan_laba_rugi_pd['LaporanKeuangan'] = 'Laba Rugi'
laporan_arus_kas_pd['LaporanKeuangan'] = 'Arus Kas'
laporan_posisi_keuangan_pd['LaporanKeuangan'] = 'Posisi Keuangan'

# Rename columns
laba_rugi_rename_map = {
    'Unnamed: 0': 'LaporanDetail',
    'Unnamed: 1': 'CurrentYearInstant',
    'Unnamed: 2': 'PriorYearInstant'
}
arus_kas_rename_map = laba_rugi_rename_map
posisi_keuangan_rename_map = laba_rugi_rename_map

laporan_laba_rugi_pd.rename(columns=laba_rugi_rename_map, inplace=True)
laporan_arus_kas_pd.rename(columns=arus_kas_rename_map, inplace=True)
laporan_posisi_keuangan_pd.rename(columns=posisi_keuangan_rename_map, inplace=True)

# Reorder columns to place 'ID', 'emitent', and 'LaporanKeuangan' first
laporan_laba_rugi_pd = laporan_laba_rugi_pd[['ID', 'emitent', 'LaporanKeuangan', 'LaporanDetail', 'CurrentYearInstant', 'PriorYearInstant']]
laporan_arus_kas_pd = laporan_arus_kas_pd[['ID', 'emitent', 'LaporanKeuangan', 'LaporanDetail', 'CurrentYearInstant', 'PriorYearInstant']]
laporan_posisi_keuangan_pd = laporan_posisi_keuangan_pd[['ID', 'emitent', 'LaporanKeuangan', 'LaporanDetail', 'CurrentYearInstant', 'PriorYearInstant']]

# Concatenate all DataFrames into one
combined_df = pd.concat([laporan_laba_rugi_pd, laporan_arus_kas_pd, laporan_posisi_keuangan_pd])

# Convert the combined Pandas DataFrame to a Dask DataFrame
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
