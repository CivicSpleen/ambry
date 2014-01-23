CREATE OR REPLACE VIEW datasets_tables AS
SELECT datasets.*, tables.*, columns.*
FROM datasets 
JOIN tables ON (tables.t_d_id = datasets.d_id)

CREATE OR REPLACE VIEW datasets_tables_columns AS
SELECT datasets.*, tables.*, columns.*
FROM datasets 
JOIN tables ON (tables.t_d_id = datasets.d_id)
JOIN columns ON (columns.c_t_id = tables.t_id);

CREATE OR REPLACE VIEW datasets_tables_columns_partitions AS
SELECT datasets.*, tables.*, columns.*, partitions.*
FROM datasets
JOIN tables ON (tables.t_d_id = datasets.d_id)
JOIN columns ON (columns.c_t_id = tables.t_id)
JOIN partitions ON partitions.p_d_id = datasets.d_id
WHERE partitions.p_t_id = tables.t_id or partitions.p_t_id is NULL;