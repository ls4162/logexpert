package insight.engineer

import java.sql.DriverManager
import java.util.Properties

/**
  * Created by sulei on 1/25/18.
  */
object SqlConnector {
  val prop = new Properties()
  val (user, password, url) = try {
    prop.load(getClass().getClassLoader().getResourceAsStream("rds.properties"))
    (
      prop.getProperty("user"),
      prop.getProperty("password"),
      prop.getProperty("url")
      )
  } catch { case e: Exception =>
    e.printStackTrace()
    sys.exit(1)
  }
  val conn= DriverManager.getConnection(url,user,password)
  val del = conn.prepareStatement ("INSERT INTO stream_session (ip,session_type,start_time,last_time,cur_time,page) VALUES (?,?,?,?,?,?)")
}