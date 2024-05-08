import sun.font.TrueTypeFont

import java.io.{File, FileOutputStream, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import scala.io.{BufferedSource, Source}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import java.sql.{Connection, DriverManager, PreparedStatement}




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

      def rule5(quantity: String, payment_method: String): Int = {
        try {
          if (payment_method.toLowerCase.trim == "app") {
            logExecution("rule5: Processed and inserted to the result", success = true)
            return (math.ceil(quantity.toDouble / 5) * 5.0).toInt
          } else {
            logExecution("rule5: not qualified", success = true)
            return 0
          }

        } catch {
          case ex: Exception =>
            logExecution("rule5: Failed " + ex.getMessage, success = false)
            0
        }
      }
        def rule6(payment:String):Int = {
          try {
            if (payment.toLowerCase.trim == "visa") {
              logExecution("rule6: Processed and inserted to the result", success = true)
              5
            } else {
              logExecution("rule6: not qualified", success = true)
              0
            }

          } catch {
            case ex: Exception =>
              logExecution("rule6: Failed "+ ex.getMessage, success = false)
              0
          }
        }






  def avg_discount(ordersList: List[String]): Float = {
    try{
    val discountedValues = List(
      rule1(ordersList(0), ordersList(2)),
      rule2(ordersList(1)),
      rule3(ordersList(0), "23-3-2023"),
      rule4(ordersList(3)),
      rule5(ordersList(3), ordersList(5)),
      rule6(ordersList(6))
    )
    val avg_discount = if (discountedValues.size >= 2) discountedValues.sorted.reverse.take(2).sum / 2.toFloat
    else if (discountedValues.size == 1) discountedValues.head
    else 0
    avg_discount
    }catch {
      case ex: Exception =>
        logExecution("Average_function: Failed " + ex.getMessage, success = false)
        0
    }}


    def applydiscount(orderslist:List[String] ,avg_discount:List[String] => Float): List[Float] = {
    val discount = avg_discount(orderslist)
    val price = orderslist(4).toFloat * orderslist(3).toFloat
    val final_price = (price * (100-discount))/100
    List(discount,final_price)
  }
  // Process each line
  val finalList = lines.map { line =>
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
  val url = "jdbc:oracle:thin:@//localhost:1521/XE"
  val driver = "oracle.jdbc.OracleDriver"
  val username = "hr"
  val password = "123"
  val connection = connectToDatabase(url, username, password)


  def connectToDatabase(url: String, username: String, password: String): Connection = {
    Class.forName("oracle.jdbc.driver.OracleDriver") // Load Oracle JDBC driver class
    DriverManager.getConnection(url, username, password) // Create connection
  }


  def insertData(connection: Connection, discount: Float, finalPrice: Float): Unit = {
    val query = "INSERT INTO discount (discount, finalprice) VALUES (?, ?)" // Replace with your table name
    val preparedStatement: PreparedStatement = connection.prepareStatement(query)
    preparedStatement.setFloat(1, discount)
    preparedStatement.setFloat(2, finalPrice)
    preparedStatement.executeUpdate()
    preparedStatement.close()
  }
  finalList.foreach { row =>
    val discount = row(0)
    val finalPrice = row(1)
    insertData(connection, discount, finalPrice)
  }
  connection.close()

  }



