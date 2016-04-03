
MATERIALIZE build.example.com-sql-integers-2;
MATERIALIZE build.example.com-sql-integers-3;

CREATE VIEW int100 AS SELECT * FROM build.example.com-sql-integers-1;

CREATE VIEW joined_integers AS
SELECT * FROM build.example.com-sql-integers-2 as p1
LEFT JOIN build.example.com-sql-integers-3 AS p2 ON p1.a = p2.a
;
