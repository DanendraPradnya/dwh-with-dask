CREATE DATABASE financial_statement;

CREATE TABLE laporan_laba_rugi (
    ID INT PRIMARY KEY,
    emitent VARCHAR(255),
    `Laporan Laba Rugi` VARCHAR(255),
    CurrentYearInstant DECIMAL(20, 2),
    PriorYearInstant DECIMAL(20, 2)
);

CREATE TABLE laporan_arus_kas (
    ID INT PRIMARY KEY,
    emitent VARCHAR(255),
    `Laporan Arus Kas` VARCHAR(255),
    CurrentYearInstant DECIMAL(20, 2),
    PriorYearInstant DECIMAL(20, 2)
);

CREATE TABLE laporan_posisi_keuangan (
    ID INT PRIMARY KEY,
    emitent VARCHAR(255),
    `Laporan Posisi Keuangan` VARCHAR(255),
    CurrentYearInstant DECIMAL(20, 2),
    PriorYearInstant DECIMAL(20, 2)
);
