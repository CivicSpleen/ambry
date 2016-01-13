-- simple example of the bundle.sql content. Stuff created here is available from sources.csv --

-- Example of a table creation. ---
CREATE TABLE table1 (
    col1 INTEGER,
    col2 INTEGER,
    col3 TEXT);
INSERT INTO table1 VALUES (1, 1, 'one');
INSERT INTO table1 VALUES (2, 2, 'two');

-- Example of a view creation. --
CREATE VIEW view1 AS
SELECT s1.id as s1_id, s2.id as s2_id FROM example.com-simple-simple AS s1
LEFT JOIN example.com-simple-simple AS s2 on s1.id = s2.id;

-- Example of a materialized view creation. --
-- It is a table for sqlite and materialized view for postgres. --
CREATE MATERIALIZED VIEW materialized_view1 as
SELECT s1.id as s1_id, s2.id as s2_id FROM example.com-simple-simple AS s1
LEFT JOIN example.com-simple-simple AS s2 on s1.id = s2.id;

-- Example of an index creation. --
INDEX example.com-simple-simple (id, uuid);