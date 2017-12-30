-- sorted for 10^6
create(db,"db1")
create(tbl,"tbl_sorted_6",db1,1)
create(col,"col1",db1.tbl_sorted_6)
create(idx,db1.tbl_sorted_6.col1,sorted,unclustered)
load("/Users/tyrocca/Code/cs165/src/experiments/sorted_10_6.csv")
shutdown
