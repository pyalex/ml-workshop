import java.util.Date
import java.text.SimpleDateFormat

import com.spotify.scio._
import com.spotify.scio.bigquery._
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.joda.time.{Duration, Instant}
import org.joda.time.format.DateTimeFormat
import utils.MailUtils

import scala.util.Try

object Thread2 {
  private val dateFormat = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z")
  //private val dateFormat = DateTimeFormat.forPattern("EEE, d MMM yyyy HH:mm:ss")
  private val windowSize = Duration.standardDays(3)
  private val windowPeriod = Duration.standardDays(1)

  case class Row(date: Date, subject: String, from: String, to: Array[String])

  def normalizeSubject(subject: String): String = {
    subject.toLowerCase.replaceAll("re:", "").trim
  }

  def addToThread(thread: Seq[Row], row: Row): Seq[Row] = {
    if (thread.map(_.date).contains(row.date)) {
      thread
    } else {
      thread :+ row
    }
  }

  def ThreadPlusThread(thread: Seq[Row], that: Seq[Row]): Seq[Row] = {
    if (that.isEmpty) {
      thread
    } else {
      ThreadPlusThread(addToThread(thread, that.head), that.tail)
    }
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val reader = TextIO.read().from(args("input")).withDelimiter("\nMessage-ID: <".getBytes)
    val in = sc.wrap(sc.pipeline.apply(reader))
      .map(_.split("\n").dropRight(1).mkString("\n")) // drop last line
      .filter(_.length > 0)
      .map("Message-ID" ++ _.trim)

    val emailParser = MailUtils.parseEmail(encoded = false)(_)

    val emails = in.flatMap(x =>
      Try({
        val email = emailParser(x)
        Row(dateFormat.parse(email.getHeader("Date").head), email.getHeader("Subject").head,
            email.getHeader("From").head, email.getHeader("To").head.split(","))
      }).toOption
    )
      .filter(_.to.length == 1)
      .timestampBy(r => new Instant(r.date.getTime / 1000))
      .withSlidingWindows(windowSize, windowPeriod)
      .map(x => ((normalizeSubject(x.subject), (x.to :+ x.from).toSet), x))
      .aggregateByKey(Seq[Row]())(addToThread, ThreadPlusThread)
    //          .map(_.length)
//      .countByValue.filter { case (_, v) => v > 1 }
        .take(100)
      .withWindow[IntervalWindow]
      .saveAsTextFile(args("output"))


    //val res = emails.take(5).materialize

    sc.close().waitUntilFinish()
    //res.waitForResult().value.foreach(println)
  }
}
