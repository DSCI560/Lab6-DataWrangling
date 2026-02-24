-- Active: 1771910848211@@127.0.0.1@3306@oil_wells
ALTER TABLE drillingedge_extra
ADD UNIQUE (well_id, scraped_url);