import dask.dataframe as dd
import pandas as pd
from sqlalchemy import create_engine
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load the Excel file
file_path = 'Data/FinancialStatement-2024-I-ACES.xlsx'

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
laporan_laba_rugi_pd.drop(columns=['Unnamed: 3'], inplace=True)  # Example of dropping a column
laporan_arus_kas_pd.drop(columns=['Unnamed: 3'], inplace=True)
laporan_posisi_keuangan_pd.drop(columns=['Unnamed: 3'], inplace=True)

# Rename
laba_rugi_rename_map = {
'Unnamed: 0':'Laporan Laba Rugi',
'Unnamed: 1':'CurrentYearInstant',
'Unnamed: 2':'PriorYearInstant'
}
arus_kas_rename_map = {
'Unnamed: 0':'Laporan Arus Kas',
'Unnamed: 1':'CurrentYearInstant',
'Unnamed: 2':'PriorYearInstant'
}
posisi_keuangan_rename_map = {
'Unnamed: 0':'Laporan Posisi Keuangan',
'Unnamed: 1':'CurrentYearInstant',
'Unnamed: 2':'PriorYearInstant'
}

laporan_laba_rugi_pd.rename(columns=laba_rugi_rename_map, inplace=True)
laporan_arus_kas_pd.rename(columns=arus_kas_rename_map, inplace=True)
laporan_posisi_keuangan_pd.rename(columns=posisi_keuangan_rename_map, inplace=True)

# Convert Pandas DataFrames to Dask DataFrames
laporan_laba_rugi = dd.from_pandas(laporan_laba_rugi_pd, npartitions=1)
laporan_arus_kas = dd.from_pandas(laporan_arus_kas_pd, npartitions=1)
laporan_posisi_keuangan = dd.from_pandas(laporan_posisi_keuangan_pd, npartitions=1)

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

# Save Dask DataFrames to MySQL
try:
    laporan_laba_rugi.compute().to_sql('laporan_laba_rugi', con=engine, if_exists='replace', index=False)
    laporan_arus_kas.compute().to_sql('laporan_arus_kas', con=engine, if_exists='replace', index=False)
    laporan_posisi_keuangan.compute().to_sql('laporan_posisi_keuangan', con=engine, if_exists='replace', index=False)
    logger.info("Data berhasil disimpan ke dalam database MySQL.")
except Exception as e:
    logger.error(f"Failed to save DataFrames to MySQL: {e}")
