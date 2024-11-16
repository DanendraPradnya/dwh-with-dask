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
pdf_path = r"C:\Users\Sgrtamade\Kuliah\Pangkalan Data\dwh-with-dask\Data\PT Ace Hardware Indonesia GA 31 Des 2023.pdf"
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
    """Determine the quarter from a date string in the format 'Per 31 Desember 2023'."""
    month_map = {
        'Januari': 1, 'Februari': 2, 'Maret': 3,
        'April': 4, 'Mei': 5, 'Juni': 6,
        'Juli': 7, 'Agustus': 8, 'September': 9,
        'Oktober': 10, 'November': 11, 'Desember': 12
    }
    
    # Adjusting the regex pattern to capture the "Per DD Month YYYY" format
    match = re.search(r'Per (\d{1,2}) (\w+) (\d{4})', date_str)
    if match:
        _, month, year = match.groups()
        month_num = month_map.get(month, 0)
        if month_num:
            quarter = (month_num - 1) // 3 + 1
            return f"Q{quarter} {year}"
    return 'Unknown'

def extract_section(text, start_marker, end_marker):
    """Extract a section of text between start and end markers."""
    start = text.find(start_marker)
    end = text.find(end_marker, start)
    if start != -1 and end != -1:
        return text[start + len(start_marker):end].strip()
    return ""

def text_to_dataframe(text, grup_lk_label, source_label, quarter):
    """Convert structured text to a DataFrame."""
    data = []
    lines = text.split('\n')
    for line in lines:
        match = re.match(r"(.+?)\s+([\d,.]+)\s*(.*)", line)
        if match:
            item, value, notes = match.groups()
            data.append([grup_lk_label, source_label, clean_text(item, max_length=255), 
                         float(value.replace(",", "")), clean_text(notes), quarter])
    if not data:
        logger.warning("No data extracted from text.")
    return pd.DataFrame(data, columns=['grup_lk', 'source', 'item', 'value', 'notes', 'quarter'])

def extract_pdf_data(pdf_path):
    """Extract data and quarter information from a PDF."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            pages = pdf.pages
            text_data = "".join([page.extract_text() for page in pages])
        
        # Look for the date in the format 'Per 31 Desember 2023'
        date_match = re.search(r'Per \d{1,2} \w+ \d{4}', text_data)
        quarter = determine_quarter(date_match.group(0)) if date_match else 'Unknown'
        
        laba_rugi = extract_section(text_data, "Laporan laba rugi", "Laporan arus kas")
        arus_kas = extract_section(text_data, "Laporan arus kas", "Laporan neraca")
        neraca = extract_section(text_data, "Laporan neraca", "Catatan atas laporan")
        
        pdf_dfs = {
            'Laba Rugi': text_to_dataframe(laba_rugi, 'Laba Rugi', 'PDF', quarter),
            'Arus Kas': text_to_dataframe(arus_kas, 'Arus Kas', 'PDF', quarter),
            'Neraca': text_to_dataframe(neraca, 'Posisi Keuangan', 'PDF', quarter)
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
            df.rename(columns={df.columns[0]: 'notes', df.columns[1]: 'CurrentYearInstant', df.columns[2]: 'PriorYearInstant'}, inplace=True)
            df['grup_lk'] = grup_lk_label
            df['source'] = 'Excel'
            df['notes'] = df['notes'].apply(lambda x: clean_text(x, max_length=255))
            df['CurrentYearInstant'] = pd.to_numeric(df['CurrentYearInstant'], errors='coerce').fillna(0)
            df['PriorYearInstant'] = pd.to_numeric(df['PriorYearInstant'], errors='coerce').fillna(0)
            df['emitent'] = emitent_value
            return df[['grup_lk', 'source', 'notes', 'CurrentYearInstant', 'PriorYearInstant', 'emitent']]
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

# Combine all data into a single DataFrame
combined_df = pd.concat([pdf_data, excel_data], ignore_index=True)

# Convert to Dask DataFrame and save to MySQL
dask_df = dd.from_pandas(combined_df, npartitions=1)
try:
    engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
    dask_df.compute().to_sql('financial_statement', con=engine, if_exists='replace', index=False)
    logger.info("Data successfully saved to the MySQL database.")
except Exception as e:
    logger.error("Failed to save to MySQL: %s", e)
