-- Active: 1771910848211@@127.0.0.1@3306@oil_wells
ALTER TABLE stimulations
ADD INDEX (well_id);

ALTER TABLE stimulations 
ADD COLUMN treatment_type VARCHAR(128) NULL;

ALTER TABLE stimulations 
ADD COLUMN lbs_proppant BIGINT NULL;

ALTER TABLE stimulations 
ADD COLUMN acid_percent FLOAT NULL;

ALTER TABLE stimulations 
MODIFY COLUMN treatment_pressure DOUBLE NULL;

ALTER TABLE stimulations 
MODIFY COLUMN max_treatment_rate DOUBLE NULL;