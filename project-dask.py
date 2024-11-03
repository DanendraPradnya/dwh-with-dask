import pdfplumber
import pandas as pd
import dask.dataframe as dd

def clean_numeric_value(value):
    """Cleans numeric values by removing commas and converting negative values."""
    return value.replace(',', '').replace('(', '-').replace(')', '')

def extract_financial_data(pdf_path):
    """Extracts financial data from a PDF and organizes it into a dictionary."""
    financial_data = {
        'Posisi Keuangan': [],
        'Laba Rugi': [],
        'Arus Kas': []
    }
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            table = page.extract_table()

            # Check if the page has a table and is relevant
            if table and len(table) > 1:
                headers = table[0]  # Assume the first row is the header

                # Determine the type of financial statement
                if "Statement of financial position" in text:
                    section = 'Posisi Keuangan'
                elif "Statement of profit or loss" in text:
                    section = 'Laba Rugi'
                elif "Statement of cash flows" in text:
                    section = 'Arus Kas'
                else:
                    continue  # Skip pages that do not match any section
                
                # Append data rows to the appropriate section
                for row in table[1:]:
                    if len(row) == len(headers):  # Ensure row matches header length
                        try:
                            row_data = {headers[i]: clean_numeric_value(row[i]) for i in range(len(headers))}
                            financial_data[section].append(row_data)
                        except Exception as e:
                            print(f"Error processing row: {row} - {e}")
    
    return financial_data

def create_dask_dataframes(financial_data):
    """Creates Dask DataFrames from the financial data dictionary."""
    dask_dfs = {}
    
    for report_type, data in financial_data.items():
        if data:
            pdf = pd.DataFrame(data)
            ddf = dd.from_pandas(pdf, npartitions=2)
            dask_dfs[report_type] = ddf
            
    return dask_dfs

def export_to_excel(dask_dfs, output_path):
    """Exports Dask DataFrames to an Excel file with each report type on a separate sheet."""
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            data_written = False  # Track if any data was written
            
            for report_type, ddf in dask_dfs.items():
                df = ddf.compute()  # Convert Dask DataFrame to Pandas DataFrame
                if not df.empty:
                    
                    # Write to Excel with header
                    df.to_excel(writer, sheet_name=report_type, index=False)
                    data_written = True
            
            # If no data was written, add a default sheet
            if not data_written:
                pd.DataFrame(["No data available"]).to_excel(writer, sheet_name="No Data", index=False)
                
    except Exception as e:
        print(f"Failed to export to Excel: {e}")

# File input and output paths
pdf_path = "C:/Users/Lenovo/Documents/DWH/project-dask/Data/FinancialStatement-2024-II-BBRI.pdf"
output_path = "Laporan_Keuangan_BBRI_2024.xlsx"

# Extract, process, and export data to Excel
financial_data = extract_financial_data(pdf_path)
dask_dfs = create_dask_dataframes(financial_data)
export_to_excel(dask_dfs, output_path)

print(f"Data telah berhasil diekspor ke {output_path}")