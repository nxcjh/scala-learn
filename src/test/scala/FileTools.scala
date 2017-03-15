
import scala.io.Source
import scala.reflect.io.Path


/**
  * Created by wanzer on 2017/3/15.
  */
object FileTools {

  def main(args: Array[String]): Unit = {
    val files = Path("e:\\data").walkFilter(p => p.isFile && p.name.contains("transaction"))
    for (file <- files){
      val reader = Source.fromFile(file.toString(), "UTF-8")

      for (line <- reader.getLines()){
        println(line)
      }
      reader.close();
      file.deleteIfExists()

    }
  }
}
