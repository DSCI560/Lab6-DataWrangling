-- Active: 1771910848211@@127.0.0.1@3306@oil_wells
CREATE TABLE wells (
  id INT AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255),          -- source PDF filename
  file_hash VARCHAR(64),          -- to detect reprocessing
  api VARCHAR(64),
  well_name VARCHAR(255),
  well_number VARCHAR(64),
  address TEXT,
  latitude DOUBLE,
  longitude DOUBLE,
  county VARCHAR(128),
  state VARCHAR(64),
  operator VARCHAR(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (file_hash)
);

CREATE TABLE stimulations (
  id INT AUTO_INCREMENT PRIMARY KEY,
  well_id INT,
  date_stimulated DATE,
  stimulated_formation VARCHAR(255),
  top_ft INT,
  bottom_ft INT,
  stages INT,
  volume DOUBLE,
  volume_units VARCHAR(64),
  treatment_pressure DOUBLE,
  max_treatment_rate DOUBLE,
  additional_info TEXT,
  FOREIGN KEY (well_id) REFERENCES wells(id) ON DELETE CASCADE
);

CREATE TABLE drillingedge_extra (
  id INT AUTO_INCREMENT PRIMARY KEY,
  well_id INT,
  well_status VARCHAR(64),
  well_type VARCHAR(128),
  closest_city VARCHAR(128),
  barrels_of_oil VARCHAR(64),
  mcf_gas VARCHAR(64),
  scraped_url VARCHAR(512),
  scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (well_id) REFERENCES wells(id) ON DELETE CASCADE
);