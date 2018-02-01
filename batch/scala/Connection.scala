package insight.engineer

import java.util.Properties

/**
  * Created by sulei on 1/31/18.
  */
object Connection {
  val prop = new Properties()
  val (driver, user, password, url, table) = try {
    prop.load(getClass().getClassLoader().getResourceAsStream("rds.properties"))
    (
      prop.getProperty("driver"),
      prop.getProperty("user"),
      prop.getProperty("password"),
      prop.getProperty("url"),
      prop.getProperty("table")
      )
  } catch { case e: Exception =>
    e.printStackTrace()
    sys.exit(1)
  }
}