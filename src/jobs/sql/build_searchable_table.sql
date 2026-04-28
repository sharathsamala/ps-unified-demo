-- Build the source table for the parts catalog Vector Search index.
-- Synthesizes a `description` text column from the silver_parts row so the
-- index has something semantic to embed. CDF is required for DELTA_SYNC.
-- :catalog — target catalog name.

USE CATALOG IDENTIFIER(:catalog);

CREATE OR REPLACE TABLE parts.parts_searchable
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS
SELECT
  part_id,
  sku,
  part_name,
  category,
  manufacturer,
  list_price_usd,
  hazmat_flag,
  CONCAT_WS(' ',
    part_name,
    CONCAT('Category:', category),
    CONCAT('Manufactured by', manufacturer),
    CASE WHEN hazmat_flag = 1 THEN 'Hazardous material handling required.' ELSE '' END,
    CONCAT('List price USD', CAST(list_price_usd AS STRING))
  ) AS description
FROM parts.silver_parts;

ALTER TABLE parts.parts_searchable
  SET TAGS ('domain' = 'parts', 'purpose' = 'vector_search_source');
