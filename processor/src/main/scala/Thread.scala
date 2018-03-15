import javax.mail.internet.MimeMessage

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection
import org.joda.time.{DateTime, Duration}
import utils.MailUtils

import utils.Data._
import utils.Parser
import utils.EmailReader

object Thread {
  private val windowSize = Duration.standardDays(3)
  private val windowPeriod = Duration.standardDays(1)
  private val parser = MailUtils.parseEmail(encoded = false)(_)

  def merge(inbounds: Iterable[Email], outbound: Iterable[Email]): Iterable[(String, Email)] = {
    (inbounds.map(("i", _)) ++ outbound.map(("o", _))).toList.sortBy(_._2.date)(Ordering.fromLessThan(_ isBefore _))
  }

  def filterResponses(merged: Iterable[(String, Email)]): Iterable[(Email, Email)] = {
    merged.sliding(2).flatMap({
      case ("i", i: Email) :: ("o", o: Email) :: Nil => Some(i, o)
      case _ => None
    }).toSeq
  }

  def findPairs(input: SCollection[Email], originator: String): SCollection[(Email, Email)] = {
    val emailsWindowed = input.timestampBy(_.date)
      .withFixedWindows(windowPeriod)

    val inbounds = emailsWindowed.filter(_.from != originator)
      .groupBy(x => (Parser.normalizeSubject(x.subject), x.from))
    val outbounds = emailsWindowed.filter(x => x.from == originator && x.to.nonEmpty)
      .groupBy(x => (Parser.normalizeSubject(x.subject), x.to.head))

    inbounds.join(outbounds).values
      .map { case (i, o) => merge(i, o) }
      .flatMap(filterResponses)
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val originator = args.getOrElse("originator", "vince.kaminski@enron.com")
    implicit val parser: (String => MimeMessage) = MailUtils.parseEmail(encoded = false)(_)

    val in = EmailReader.readSample(sc, args("input")).map(Parser.parseEmail).flatten

    val res = findPairs(in, originator)
      .map(x => (x._1.body.split("\n\n\n")(0), x._2.body.split("\n\n\n")(0))).materialize

    sc.close()

    res.waitForResult().value.foreach(println)
  }
}