
val l1 : Seq[Int] = Seq(5,3,2,4,1).sortWith((a,b) => a < b)
val l2 : Seq[Int] = Seq(6,7,8,8)


val l3 = l1 ++ l2

l3.take(5)
l3.slice(5, l3.size)
