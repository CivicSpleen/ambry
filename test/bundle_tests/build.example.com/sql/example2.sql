
MATERIALIZE build.example.com-sql-integers-2;
MATERIALIZE build.example.com-sql-integers-3;

SELECT * FROM build.example.com-sql-integers-2 as p1
LEFT JOIN build.example.com-sql-integers-3 AS p2 ON p1.a = p2.a;
