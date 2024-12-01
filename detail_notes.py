import pdfplumber
import pandas as pd
import re
import logging
from sqlalchemy import create_engine, Column, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the base class for the ORM model
Base = declarative_base()
class CALKData(Base):
    __tablename__ = 'detail_notes'

    id = Column(String(255), primary_key=True, autoincrement=True)  # Pastikan id memiliki autoincrement
    Nomor = Column(String(255), nullable=False)  # Ganti 'no' menjadi 'Nomor'
    calk = Column(String(255), nullable=False)
    deskripsi = Column(Text, nullable=True)


def extract_calk(pdf_path):
    data = []
    current_no = None
    current_calk = None
    current_desc = []
    is_calk_section = False

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            lines = text.split('\n')

            for line in lines:
                line = line.strip()

                # Deteksi awal "Catatan Atas Laporan Keuangan"
                if "Catatan Atas Laporan Keuangan" in line:
                    is_calk_section = True
                    continue

                # Jika belum di bagian CALK, lanjutkan
                if not is_calk_section:
                    continue

                # Deteksi format nomor CALK (1., 1.a., 1.a.i.)
                match = re.match(r'^(\d+(\.\w+)*\.)\s+(.*)', line)
                if match:
                    # Simpan data CALK sebelumnya
                    if current_no and current_calk:
                        data.append({
                            "Nomor": current_no,
                            "CALK": current_calk,
                            "Deskripsi": " ".join(current_desc)
                        })

                    # Mulai CALK baru
                    current_no = match.group(1).strip()  # Nomor CALK
                    current_calk = match.group(3).strip()  # Judul CALK
                    current_desc = []  # Reset deskripsi
                    continue

                # Tambahkan ke deskripsi jika tidak ada nomor baru
                if current_no and current_calk:
                    current_desc.append(line)

    # Tambahkan CALK terakhir
    if current_no and current_calk:
        data.append({
            "Nomor": current_no,
            "CALK": current_calk,
            "Deskripsi": " ".join(current_desc)
        })

    return data

def save_to_database(data):
    try:
        # SQLAlchemy connection setup
        engine = create_engine('mysql+pymysql://root:@localhost/financial_statement', echo=True)
        Base.metadata.create_all(engine)

        # Create session
        Session = sessionmaker(bind=engine)
        session = Session()

        # Menyimpan data ke dalam tabel
        for row in data:
            calk_record = CALKData(Nomor=row["Nomor"], calk=row["CALK"], deskripsi=row["Deskripsi"])
            session.add(calk_record)

        # Commit transaksi
        session.commit()

        # Log jika berhasil menyimpan data
        logging.info(f"{len(data)} data berhasil disimpan ke database.")
        
        # Tutup session
        session.close()
        
    except Exception as e:
        logging.error(f"Terjadi kesalahan: {e}")


# Path file PDF
file_path = "Data/PT Ace Hardware Indonesia GA 31 Des 2023.pdf"

# Ekstraksi data
calk_data = extract_calk(file_path)

# Simpan data ke database
save_to_database(calk_data)

# Tampilkan hasil ekstraksi untuk pengecekan
df = pd.DataFrame(calk_data)
print(df)
