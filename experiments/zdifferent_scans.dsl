-- Test Select + Fetch
--
-- SELECT col1 FROM tbl2 WHERE col4 >= -1000 AND col4 < 1200;

s4=select(db1.tbl2.col4, -2222, 1147482993)
print(s4)

s4=select(db1.tbl2.col4, -2222, 1600000000)
print(s4)

s4=select(db1.tbl2.col4, -2222, 2000000000)
print(s4)

