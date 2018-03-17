package legacy

import javax.mail.internet.MimeMessage

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.tensorflow._
import com.spotify.scio.values.SCollection
import legacy.Features.{FeaturesRow, simpleFeatures, withTitle}
import org.joda.time.Duration
import shapeless.datatype.tensorflow._
import utils.Data.Email
import utils.{EmailReader, MailUtils, Parser}

object Batch {
  def withThreadFeatures(emails: SCollection[Email], originator: String)(features: SCollection[FeaturesRow]): SCollection[FeaturesRow] = {
    val emailsByThread = emails
      .groupBy(_.threadId.getOrElse(""))
      .mapValues(_.toList.sortBy(_.date)(Ordering.fromLessThan(_ isBefore _)).zipWithIndex).flattenValues

    val no_in_thread = emailsByThread.values.map(x => (x._1.messageId, x._2))
    val first_answer_index = emailsByThread
      .filter { case (thread, (e, i)) => e.from == originator }.mapValues(_._2).minByKey.asMapSideInput

    features
      .map(x => (x.messageId, x))
      .leftOuterJoin(no_in_thread)
      .values.map {
      case (l, r) => l.copy(no_in_thread = r.getOrElse(-1), first_in_thread = r.getOrElse(-1) == 0)
    }

      .withSideInputs(first_answer_index)
      .map { case (f, ctx) => f.copy(
        thread_has_previous_answer = ctx(first_answer_index).get(f.threadId).exists(_ < f.no_in_thread))
      }
      .toSCollection
  }

  def withAnsweredFeatures(emails: SCollection[Email], originator: String)(features: SCollection[FeaturesRow]): SCollection[FeaturesRow] = {
    val answers = emails.filter(_.from == originator).groupBy(_.inReplyTo.getOrElse("")).mapValues(_ => true).debug()

    //features.map(_.messageId).debug()
    features.map(x => (x.messageId, x)).leftOuterJoin(answers)
      .mapValues { case (l, r) => l.copy(answered = r.getOrElse(false)) }.values
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val originator = args.getOrElse("originator", "moskalenko.alexey@gmail.com")
    implicit val parser: (String => MimeMessage) = MailUtils.parseEmail(encoded = false)(_)

    val emails = EmailReader.readMbox(sc, args("input")).map(Parser.parseEmail).flatten
      .timestampBy(_.date).withFixedWindows(Duration.standardDays(1))

    val inbounds = emails.filter(_.from != originator)

    val prepareFeatures: (SCollection[Email] => SCollection[FeaturesRow]) =
      simpleFeatures(originator) _ andThen
        withAnsweredFeatures(emails, originator) andThen
        withThreadFeatures(emails, originator) andThen
        withTitle(inbounds, sc)

    val features = prepareFeatures(emails)
    val sampledFeatures = features.filter(_.answered) ++ features.filter(!_.answered).sample(false, 0.02)

    val featuresType = TensorFlowType[FeaturesRow]
    sampledFeatures
      .map(t => featuresType.toExample(t).toByteArray)
      .saveAsTfRecordFile(args("output"))

    val res = features.filter(_.answered).count.materialize

    sc.close()
    res.waitForResult().value.foreach(println)
  }
}
