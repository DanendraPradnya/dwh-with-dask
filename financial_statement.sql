CREATE DATABASE financial_statement;

CREATE TABLE laporan_keuangan (
    ID INT PRIMARY KEY,
    emitent VARCHAR(255),
    LaporanKeuangan VARCHAR(50), -- Menunjukkan jenis laporan (Laba Rugi, Arus Kas, atau Posisi Keuangan)
    LaporanDetail VARCHAR(255),
    CurrentYearInstant DECIMAL(20, 2),
    PriorYearInstant DECIMAL(20, 2)
);

