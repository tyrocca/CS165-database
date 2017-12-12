-- btree for 10^4
create(db,"db1")
create(tbl,"tbl_btree_4",db1,1)
create(col,"col1",db1.tbl_btree_4)
create(idx,db1.tbl_btree_4.col1,btree,unclustered)
load("/Users/tyrocca/Code/cs165/src/experiments/btree_10_4.csv")
shutdown
