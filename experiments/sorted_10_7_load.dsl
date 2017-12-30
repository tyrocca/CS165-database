-- sorted for 10^7
create(db,"db1")
create(tbl,"tbl_sorted_7",db1,1)
create(col,"col1",db1.tbl_sorted_7)
create(idx,db1.tbl_sorted_7.col1,sorted,unclustered)
load("/Users/tyrocca/Code/cs165/src/experiments/sorted_10_7.csv")
shutdown
