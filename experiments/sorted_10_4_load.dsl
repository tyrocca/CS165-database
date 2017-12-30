-- sorted for 10^4
create(db,"db1")
create(tbl,"tbl_sorted_4",db1,1)
create(col,"col1",db1.tbl_sorted_4)
create(idx,db1.tbl_sorted_4.col1,sorted,unclustered)
load("/Users/tyrocca/Code/cs165/src/experiments/sorted_10_4.csv")
shutdown
