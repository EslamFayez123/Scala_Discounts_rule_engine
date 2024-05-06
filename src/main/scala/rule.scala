import sun.font.TrueTypeFont

import java.io.{File, FileOutputStream, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.io.{BufferedSource, Source}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory



object rule extends App {
  var source: BufferedSource = Source.fromFile("src/main/scala/TRX1000 (1).csv")
  val logger = LoggerFactory.getLogger(getClass)

  val lines: List[String] = source.getLines().drop(1).toList // drop header

  val f: File = new File("src/main/scala/RESULT.csv")
  val writer = new PrintWriter(new FileOutputStream(f,false))

  def logExecution(logMessage: => String, success: Boolean): Unit = {
    if (success) {
        logger.info(logMessage)
    } else {
      logger.debug(logMessage)
    }
  }

  def avg_discount(orderslist: List[String]): Float = {
    try {
      def rule1(startdate: String, enddate: String): Int = {
        try {
          val format = new SimpleDateFormat("yyyy-MM-dd")
          val date_s = format.parse(startdate)
          val date_e = format.parse(enddate)
          val daysBetween = ChronoUnit.DAYS.between(date_s.toInstant, date_e.toInstant)
          if (daysBetween > 30) {
            val x = 30 - daysBetween.toInt
            logExecution("rule1: Processed and inserted to the result", success = true)
            x
          } else {
            logExecution("rule1: not qualified ", success = true)
            0
          }
        } catch {
          case ex: Exception =>
            logExecution("rule1: Failed "+ ex.getMessage, success = false)
            0
        }
      }

      def rule2(productname: String): Int = {
        try {
          val cheese_discount = 10
          val wine_dicount = 5
          if (productname.toLowerCase.contains("cheese")) {
            logExecution("rule2: Processed and inserted to the result", success = true)
            cheese_discount
          } else if (productname.toLowerCase.contains("wine")) {
            logExecution("rule2: Processed and inserted to the result", success = true)
            wine_dicount
          } else {
            logExecution("rule2: not qualified ", success = true)
            0
          }
        } catch {
          case ex: Exception =>
            logExecution("rule2: Failed "+ ex.getMessage, success = false)
            0
        }
      }

      def rule3(dateString: String, target: String): Int = {
        try {
          val format = new SimpleDateFormat("yyyy-MM-dd")
          val date_s = format.parse(dateString)
          val date_t = format.parse(target)

          if (date_s == (date_t)) {
            logExecution("rule3: Processed and inserted to the result", success = true)
            50
          } else {
            logExecution("rule3: not qualified", success = true)
            0
          }
        } catch {
          case ex: Exception =>
            logExecution("rule3: Failed "+ ex.getMessage, success = false)
            0
        }
      }

      def rule4(unitsString: String): Int = {
        try {
          val unitsInt = unitsString.toInt
          if (unitsInt >= 6 && unitsInt <= 9) {
            logExecution("rule4: Processed and inserted to the result", success = true)
            5
          } else if (unitsInt >= 10 && unitsInt <= 14) {
            logExecution("rule4: Processed and inserted to the result", success = true)
            7
          } else if (unitsInt >= 15) {
            logExecution("rule4: Processed and inserted to the result", success = true)
            10
          } else {
            logExecution("rule4: not qualified", success = true)
            0
          }
        } catch {
          case ex: Exception =>
            logExecution("rule4: Failed "+ ex.getMessage, success = false)
            0
        }
      }
      def rule6(payment:String):Int = {
        try {
          if (payment.toLowerCase == "visa") {
            logExecution("rule5: Processed and inserted to the result", success = true)
            5
          } else {
            logExecution("rule5: not qualified", success = true)
            0
          }

        } catch {
          case ex: Exception =>
            logExecution("rule6: Failed "+ ex.getMessage, success = false)
            0
        }
      }
      def rule5(quantity: String, payment_method: String): Int = {
        try {
          if (payment_method.toLowerCase.trim == "app") {
            logExecution("rule6: Processed and inserted to the result", success = true)
            return (math.ceil(quantity.toDouble / 5) * 5.0).toInt
          } else {
            logExecution("rule6: not qualified", success = true)
            return 0
          }

        } catch {
          case ex: Exception =>
            logExecution("rule6: Failed "+ ex.getMessage, success = false)
            0
        }
        }


      val Field1 = orderslist(0)
      val field2 = orderslist(1)
      val field3 = orderslist(2)
      val field4 = orderslist(3)
      val field6 = orderslist(5)
      val field7 = orderslist(6)
      val discountedlist = List(rule1(Field1, field3), rule2(field2), rule3(Field1, "23-3-2023"), rule4(field4),rule5(field3,field6),rule6(field7))
      val avg_discount = if (discountedlist.size >= 2) discountedlist.sorted.reverse.take(2).sum / 2.toFloat
      else if (discountedlist.size == 1) discountedlist.head
      else 0

      avg_discount
    } catch {
      case ex: Exception =>
        logExecution("Error in avg_discount function: " + ex.getMessage, success = false)
        0
    }
  }

  def applydiscount(orderslist:List[String] ,avg_discount:List[String] => Float): List[Float] = {
    val discount = avg_discount(orderslist)
    val price = orderslist(4).toFloat * orderslist(3).toFloat
    val final_price = (price * (100-discount))/100
    List(discount,final_price)
  }
  // Process each line
  val finalList: List[List[Float]] = lines.map { line =>
    val fields = line.split(",").toList
    applydiscount(fields, avg_discount)
  }
  writer.println("discount,final price")

  // Write final list to CSV file
  finalList.foreach { x =>
    val line = x.mkString(",")
    writer.println(line)
  }

  writer.close()
}

