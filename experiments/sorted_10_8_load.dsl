-- sorted for 10^8
create(db,"db1")
create(tbl,"tbl_sorted_8",db1,1)
create(col,"col1",db1.tbl_sorted_8)
create(idx,db1.tbl_sorted_8.col1,sorted,unclustered)
load("/Users/tyrocca/Code/cs165/src/experiments/sorted_10_8.csv")
shutdown
