import pdfplumber
import pandas as pd
import dask.dataframe as dd
import re
from sqlalchemy import create_engine
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# File paths
pdf_path = 'Data/PT Ace Hardware Indonesia GA 31 Des 2023.pdf'
excel_path = 'Data/FinancialStatement-2024-I-ACES.xlsx'

# Database configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'financial_statement'
}

# Utility functions
def clean_text(text, max_length=None):
    """Cleans text by removing special characters and trimming to max length."""
    if not isinstance(text, str):
        text = str(text) if pd.notna(text) else ""
    cleaned_text = re.sub(r'[^\x00-\x7F]+', '', text)
    if max_length:
        cleaned_text = cleaned_text[:max_length]
    return cleaned_text.strip()

def determine_quarter(date_str):
    """Determine the quarter from a date string in the format 'Per 31 Desember 2023 dan 2022'."""
    logger.info(f"Processing date string: {date_str}")
    month_map = {
        'Januari': 1, 'Februari': 2, 'Maret': 3,
        'April': 4, 'Mei': 5, 'Juni': 6,
        'Juli': 7, 'Agustus': 8, 'September': 9,
        'Oktober': 10, 'November': 11, 'Desember': 12
    }
    match = re.search(r'Per (\d{1,2}) (\w+) (\d{4})(?: dan \d{4})?', date_str)
    if match:
        _, month, year = match.groups()
        month_num = month_map.get(month.capitalize(), 0)
        if month_num:
            quarter = (month_num - 1) // 3 + 1
            result = f"Q{quarter} {year}"
            logger.info(f"Determined quarter: {result}")
            return result
    logger.warning("Failed to determine quarter. Returning 'Unknown'.")
    return 'Unknown'

def extract_section(text, start_marker, end_marker):
    """Extract a section of text between start and end markers."""
    start = text.find(start_marker)
    end = text.find(end_marker, start)
    if start != -1 and end != -1:
        return text[start + len(start_marker):end].strip()
    return ""

def text_to_dataframe(text, grup_lk_label, quarter):
    """Convert structured text to a DataFrame."""
    logger.info(f"Processing text for {grup_lk_label}: {text[:500]}")  # Log the first 500 characters of the section
    data = []
    lines = text.split('\n')
    for line in lines:
        match = re.match(r"(.+?)\s+([\d,.]+)\s*(.*)", line)
        if match:
            item, value, _ = match.groups()
            data.append([grup_lk_label, clean_text(item, max_length=255), 
                         float(value.replace(",", "")), quarter])
        else:
            logger.warning(f"No match found for line: {line}")  # Log lines that do not match
    if not data:
        logger.warning("No data extracted from text.")
    df = pd.DataFrame(data, columns=['grup_lk', 'item', 'value', 'quarter'])
    logger.info(f"DataFrame created: {df.head()}")  # Log the DataFrame created
    return df

def extract_pdf_data(pdf_path):
    """Extract data and quarter information from a PDF."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            pages = pdf.pages
            text_data = "".join([page.extract_text() for page in pages])
        
        logger.info(f"Extracted text from PDF: {text_data[:500]}")  # Log the first 500 characters of extracted text

        # Extract all date strings
        date_matches = re.findall(r'Per \d{1,2} \w+ \d{4}(?: dan \d{4})?', text_data)
        quarters = []
        for date_str in date_matches:
            logger.info(f"Extracted date string: {date_str}")  # Log each extracted date string
            quarter = determine_quarter(date_str)
            logger.info(f"Quarter determined for '{date_str}': {quarter}")  # Log the determined quarter
            quarters.append(quarter)
        
        # Check if any quarters were found
        if quarters:
            logger.info(f"Quarters found: {quarters}")  # Log all found quarters
            quarter = quarters[-1]  # Use the latest quarter found
        else:
            logger.warning("No quarters found. Setting quarter to 'Unknown'.")
            quarter = 'Unknown'
        
        logger.info(f"Final determined quarter: {quarter}")  # Log the final quarter

        laba_rugi = extract_section(text_data, "Laporan laba rugi", "Laporan arus kas")
        logger.info(f"Laba Rugi section: {laba_rugi[:500]}")  # Log the first 500 characters of the Laba Rugi section
        arus_kas = extract_section(text_data, "Laporan arus kas", "Laporan neraca")
        neraca = extract_section(text_data, "Laporan neraca", "Catatan atas laporan")
        
        pdf_dfs = {
            'Laba Rugi': text_to_dataframe(laba_rugi, 'Laba Rugi', quarter),
            'Arus Kas': text_to_dataframe(arus_kas, 'Arus Kas', quarter),
            ' Neraca': text_to_dataframe(neraca, 'Posisi Keuangan', quarter)
        }
        return pd.concat(pdf_dfs.values(), ignore_index=True)
    except Exception as e:
        logger.error("Failed to process PDF: %s", e)
        return pd.DataFrame()

def process_excel(file_path):
    """Process Excel sheets into structured DataFrames."""
    try:
        general_info_df = pd.read_excel(file_path, sheet_name='1000000', header=None)
        emitent_value = general_info_df.loc[general_info_df[0] == 'Kode entitas', 1].values[0]
    except Exception as e:
        logger.error("Failed to extract emitent: %s", e)
        emitent_value = 'Unknown'
    
    def excel_to_dataframe(sheet_name, grup_lk_label):
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=1)
            df.rename(columns={df.columns[0]: 'item', df.columns[1]: 'value'}, inplace=True)
            df['grup_lk'] = grup_lk_label
            df['item'] = df['item'].apply(lambda x: clean_text(x, max_length=255))
            df['value'] = pd.to_numeric(df['value'], errors='coerce').fillna(0)
            df['emitent'] = emitent_value
            df['quarter'] = 'Q1 2024'  # Example: Assign quarter (adjust accordingly)
            return df[['grup_lk', 'item', 'value', 'emitent', 'quarter']]
        except Exception as e:
            logger.error("Error processing sheet %s: %s", sheet_name, e)
            return pd.DataFrame()
    
    sheets = [
        ('1311000', 'Laba Rugi'),
        ('1510000', 'Arus Kas'),
        ('1210000', 'Posisi Keuangan')
    ]
    dfs = [excel_to_dataframe(sheet, label) for sheet, label in sheets]
    return pd.concat(dfs, ignore_index=True)

# Combine PDF and Excel Data
pdf_data = extract_pdf_data(pdf_path)
excel_data = process_excel(excel_path)

# Fill any missing quarter values with 'Unknown' before combining
pdf_data['quarter'] = pdf_data['quarter'].fillna('Unknown')
excel_data['quarter'] = excel_data['quarter'].fillna('Unknown')

# Combine all data into a single DataFrame
combined_df = pd.concat([pdf_data, excel_data], ignore_index=True)

# Log the combined DataFrame before saving
logger.info(f"Combined DataFrame: {combined_df.head()}")
logger.info(f"Unique quarters: {combined_df['quarter'].unique()}")

# Add ID column for numbering starting from 1
combined_df['ID'] = pd.Series(range(1, len(combined_df) + 1))

# Move ID and emitent columns to the first and second positions respectively
combined_df = combined_df[['ID', 'emitent'] + [col for col in combined_df.columns if col not in ['ID', 'emitent']]]

# Convert to Dask DataFrame and save to MySQL
dask_df = dd.from_pandas(combined_df, npartitions=1)
try:
    engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
    dask_df.compute().to_sql('financial_statement', con=engine, if_exists='replace', index=False)
    logger.info("Data successfully saved to the MySQL database.")
except Exception as e:
    logger.error("Failed to save to MySQL: %s", e)
    logger.info(f"Data being inserted:\n{combined_df.head()}")
