val medicare_RDD1 = sc.textFile("Medicare_Geographic_Variation_by_National_State_County_2021.csv")

val columnsRDD = medicare_RDD1.map(line => line.split(","))

// Show first 10 lines of the RDD
medicare_RDD1.take(10).foreach(println)

// Count all the records in the RDD
val count = medicare_RDD1.count()

val keyValueRDD = medicare_RDD1.map(record => {
  val keyValue = record.split(",")
  (keyValue(0), keyValue(1))
})

val countByKeys = keyValueRDD.countByKey()

countByKeys.foreach(println)

/*
 * Now which columns i am going to be working with 
 *all columns till index 23  */


val distinctColumn1 = columnsRDD.map(columns => columns(0)).distinct()
distinctColumn1.collect().foreach(println)
val distinctColumn2 = columnsRDD.map(columns => columns(1)).distinct()
distinctColumn1.collect().foreach(println)
val distinctColumn3 = columnsRDD.map(columns => columns(2)).distinct()
distinctColumn3.collect().foreach(println)
val distinctColumn = columnsRDD.map(columns => columns(3)).distinct()
distinctColumn.collect().foreach(println)
val distinctColumn4 = columnsRDD.map(columns => columns(4)).distinct()
distinctColumn4.collect().foreach(println)
val distinctColumn = columnsRDD.map(columns => columns(5)).distinct()
distinctColumn.collect().foreach(println)

/*...*/
