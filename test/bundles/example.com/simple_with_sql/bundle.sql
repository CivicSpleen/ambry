-- simple example of the bundle.sql content. Stuff created here is available from sources.csv --

-- Create table which will be available from sources.csv of simple_with_sql bundle. ---
CREATE TABLE table1 (
    col1 INTEGER,
    col2 INTEGER);

-- Create view which will be available from sources.csv of simple_with_sql bundle --
CREATE VIEW view1 AS
SELECT s1.id as s1_id, s2.id as s2_id FROM example.com-simple-simple AS s1
LEFT JOIN example.com-simple-simple AS s2 on s1.id = s2.id;

-- Example of the materialized view. --
-- It is a table for sqlite and materialized view for sqlite. --
--CREATE MATERIALIZED VIEW materialized_view1 as
--SELECT s1.id as s1_id, s2.id as s2_id FROM example.com-simple-simple AS s1
--LEFT JOIN example.com-simple-simple AS s2 on s1.id = s2.id;

-- Example of the creating index.
-- INDEX example.com-simple-simple (id, uuid); FIXME: Add more fields--