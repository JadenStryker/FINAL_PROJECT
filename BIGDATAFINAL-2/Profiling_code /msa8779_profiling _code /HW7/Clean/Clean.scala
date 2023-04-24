import scala.Double.NaN
import scala.util.Try
val medicare_RDD1 = sc.textFile("Medicare_Geographic_Variation_by_National_State_County_2021.csv")

val columnsRDD = medicare_RDD1.map(line => line.split(","))

val filteredColumnsRDD = columnsRDD.map(columns => columns.slice(0, 24))

val filteredDataRDD = filteredColumnsRDD.map(columns => columns.mkString(","))


def parseNumeric(column: String): Double = {
  Try(column.toDouble).getOrElse(NaN)
}

val numericColumnsRDD = columnsRDD.map(columns => {
  val column0 = parseNumeric(columns(0))
  val column1 = columns(1)
  val column2 = columns(2)
  val column3 = parseNumeric(columns(3))
  val column4 = columns(4)
  val column5 = parseNumeric(columns(5))
  val column6 = parseNumeric(columns(6))
  val column7 = parseNumeric(columns(7))
  val column8 = parseNumeric(columns(8))
  val column9 = parseNumeric(columns(9))
  val column10 = parseNumeric(columns(10))
  val column11 = parseNumeric(columns(11))
  val column12 = parseNumeric(columns(12))
  val column13 = parseNumeric(columns(13))
  val column14= parseNumeric(columns(14))
  val column15= parseNumeric(columns(15))
  val column16= parseNumeric(columns(16))
  val column17 = parseNumeric(columns(17))
  val column18= parseNumeric(columns(18))
  val column19= parseNumeric(columns(19))
  val column20= parseNumeric(columns(20))
  val column21 = parseNumeric(columns(21))
  val column22= parseNumeric(columns(22))
  val column23= parseNumeric(columns(23))

  Array(column0, column1, column2,column3,column4,column5,column6,column7,column8,column9,column10,column11,column12,column13,column14,column15,column16,column17,column18,column19,column20,column21,column22,column23)
})

def countNaN(row: Array[Any]): Int = {
  row.count {
    case value: Double => value.isNaN
    case _ => false
  }
}

 val filteredRowsRDD = numericColumnsRDD.filter(row => countNaN(row) <= 3)

 val csvRDD = filteredRowsRDD.map(record => record.mkString(","))

 val coalescedCsvRDD = csvRDD.coalesce(1)

 coalescedCsvRDD.saveAsTextFile("final_medicare_file")

