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
pdf_path = 'C:/Users/Sgrtamade/Kuliah/Pangkalan Data/dwh-with-dask/Data/PT-Ace-Hardware-Indonesia-GA-31-Des-2023.pdf'
excel_path = 'C:/Users/Sgrtamade/Kuliah/Pangkalan Data/dwh-with-dask/Data/FinancialStatement-2024-I-ACES.xlsx'

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

import re

def determine_quarter(date_str):
    """Determine the quarter from a date string in the format 'Pada Tanggal 31 Desember 2023'."""
    logger.info(f"Processing date string: {date_str}")
    
    month_map = {
        'Januari': 1, 'Februari': 2, 'Maret': 3,
        'April': 4, 'Mei': 5, 'Juni': 6,
        'Juli': 7, 'Agustus': 8, 'September': 9,
        'Oktober': 10, 'November': 11, 'Desember': 12
    }

    # Update regex to match "Pada Tanggal 31 Desember 2023 dan 2022"
    match = re.search(r'Pada Tanggal (\d{1,2}) (\w+) (\d{4})(?: dan (\d{4}))?', date_str)

    
    if match:
        day, month, year = match.groups()[:3]
        month_num = month_map.get(month.capitalize(), 0)
        logger.info(f"Mapped month number: {month_num}")

    month_num = month_map.get(month.capitalize(), 0)
    if month_num:
        quarter = (month_num - 1) // 3 + 1
        result = f"Q{quarter} {year}"
        logger.info(f"Determined quarter: {result}")
        return result

    else:
        logger.warning(f"Failed to match date string: {date_str}")
    
    logger.warning(f"Returning 'Unknown' for date string '{date_str}'.")
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

        logger.info(f"Extracted text from PDF: {text_data[:500]}")  # Log first 500 chars of text

        # Extract all date strings (i.e., Per 31 Desember 2023)
        date_matches = re.findall(r'Per|Pada(?: Tanggal)?)\s*(\d{1,2})\s+(\w+)\s+(\d{4}', text_data)
        logger.info(f"Extracted date matches: {date_matches}")  # Log extracted date matches

        if not date_matches:
            logger.error("No date matches found. Check the extracted text for expected format.")
            logger.error(f"Full extracted text:\n{text_data[:1000]}")  # Log full extracted text if no date matches

        quarters = []
        for date_str in date_matches:
            logger.info(f"Processing date string: {date_str}")  # Log each extracted date
            quarter = determine_quarter(date_str)
            logger.info(f"Quarter determined for '{date_str}': {quarter}")  # Log the determined quarter
            quarters.append(quarter)

        # Choose the last quarter found
        if quarters:
            logger.info(f"Quarters found: {quarters}")  # Log all found quarters
            quarter = quarters[-1]  # Use the latest quarter found
        else:
            logger.warning("No quarters found. Setting quarter to 'Unknown'.")
            quarter = 'Unknown'

        logger.info(f"Final determined quarter: {quarter}")  # Log the final quarter

        laba_rugi = extract_section(text_data, "Laporan laba rugi", "Laporan arus kas")
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



def process_excel(file_path, quarter):
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
            df['quarter'] = quarter  # Set the quarter dynamically based on PDF extraction
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
# Extract PDF Data
pdf_data = extract_pdf_data(pdf_path)

# Check if the quarter is being set correctly from the PDF extraction
quarter = pdf_data['quarter'].iloc[0] if not pdf_data.empty else 'Unknown'
logger.info(f"Quarter extracted from PDF data: {quarter}")  # Log the quarter to check its value

# Process Excel data with the extracted quarter
excel_data = process_excel(excel_path, quarter)

# Check if the quarter is being added to the Excel data correctly
logger.info(f"Quarter in excel_data: {excel_data['quarter'].unique()}")  # Check quarter in Excel DataFrame

# Combine the PDF and Excel data into a single DataFrame
combined_df = pd.concat([pdf_data, excel_data], ignore_index=True)

# Ensure the quarter is correctly added to the combined DataFrame
logger.info(f"Unique quarters in combined_df: {combined_df['quarter'].unique()}")  # Check combined DataFrame for quarter

# Add ID column for numbering starting from 1
combined_df['ID'] = pd.Series(range(1, len(combined_df) + 1))

# Move ID and emitent columns to the first and second positions respectively
combined_df = combined_df[['ID', 'emitent'] + [col for col in combined_df.columns if col not in ['ID', 'emitent']]]

# Convert Pandas DataFrame to Dask DataFrame (Optional)
dask_df = dd.from_pandas(combined_df, npartitions=1)

# Add log before saving to SQL
logger.info(f"Final Combined DataFrame before saving:\n{combined_df.head()}")
logger.info(f"Unique quarters in Combined DataFrame: {combined_df['quarter'].unique()}")

# Save to MySQL
try:
    engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
    dask_df.compute().to_sql('financial_statement', con=engine, if_exists='replace', index=False)
    logger.info("Data successfully saved to the MySQL database.")
except Exception as e:
    logger.error("Failed to save to MySQL: %s", e)
    logger.info(f"Data being inserted:\n{combined_df.head()}")