package legacy

import javax.mail.internet.MimeMessage

import com.spotify.scio.avro.types.AvroType
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.joda.time.Duration
import utils.Data.Email
import utils.{EmailReader, MailUtils, Parser}
import legacy.Thread.findPairs
import legacy.SignatureProcessor.extractTitleFromSignature

/* Features:
  Simple:
  body
  body_word_count
  subject_word_count
  originator_in_cc
  amount_of_participants

  Complex:
  no in thread (first_in_thread)
  thread_has_previous_answer
  contact_title

  Dependent:
  answered(Yes/no)
*/

object Features {

  @AvroType.toSchema
  case class Title(FindPhrase: Option[String] = None,
                   FindPhraseID: Long,
                   FindPhraseStatus: Option[String] = None,
                   FindPhraseNote: Option[String] = None,
                   ReplacePhrase: Option[String] = None,
                   AssignedRoleID: Option[String] = None,
                   AssignedRole: Option[String] = None,
                   AssignedRoleRank: Long,
                   JobTitleUsageCount: Option[String] = None,
                   JobTitleSearchCount: Option[String] = None,
                   FindPhraseScore: Long)

  case class FeaturesRow(messageId: String = "",
                         threadId: String = "",
                         //datetime: Instant = Instant.now(),
                         subject: String = "",
                         body_word_count: Int = 0,
                         subject_word_count: Int = 0,
                         originator_in_cc: Boolean = false,
                         amount_of_participants: Int = 0,
                         no_in_thread: Int = 1,
                         first_in_thread: Boolean = true,
                         thread_has_previous_answer: Boolean = false,
                         contact_title: String = "",
                         body: String = "",
                         answered: Boolean = false)

  def simpleFeatures(e: Email, originator: String): FeaturesRow = FeaturesRow(
    messageId = e.messageId,
    threadId = e.threadId.getOrElse(""),
    //datetime = e.date,
    subject = e.subject,
    body_word_count = e.body.split(" ").length,
    subject_word_count = e.subject.split(" ").length,
    originator_in_cc = e.cc.contains(originator),
    amount_of_participants = (e.to ++ e.cc).length
  )

  def simpleFeatures(originator: String)(input: SCollection[Email]): SCollection[FeaturesRow] = {
    input.map(x => simpleFeatures(x, originator))
  }

  def withAnsweredFeatures(emails: SCollection[Email], originator: String)(features: SCollection[FeaturesRow]): SCollection[FeaturesRow] = {
    val answered = findPairs(emails, originator).map { case (i, _) => (i.messageId, true) }.debug()

    features.map(x => (x.messageId, x)).leftOuterJoin(answered)
      .mapValues { case (l, r) => l.copy(answered = r.getOrElse(false)) }.values
  }

  def withThreadFeatures(emails: SCollection[Email], originator: String)(features: SCollection[FeaturesRow]): SCollection[FeaturesRow] = {
    val emailsByThread = emails
      .groupBy(e => Parser.normalizeSubject(e.subject))
      .mapValues(_.toList.sortBy(_.date)(Ordering.fromLessThan(_ isBefore _)).zipWithIndex).flattenValues

    val no_in_thread = emailsByThread.values.map(x => (x._1.messageId, x._2))
    val first_answer_index = emailsByThread
      .filter { case (subject, (e, i)) => e.from == originator }.mapValues(_._2).minByKey.asMapSideInput

    features
      .map(x => (x.messageId, x))
      .leftOuterJoin(no_in_thread)
      .values.map {
      case (l, r) => l.copy(no_in_thread = r.getOrElse(-1), first_in_thread = r.getOrElse(-1) == 0)
    }

      .withSideInputs(first_answer_index)
      .map { case (f, ctx) => f.copy(
        thread_has_previous_answer = ctx(first_answer_index).get(f.subject).exists(_ < f.no_in_thread))
      }
      .toSCollection
  }

  def withTitle(emails: SCollection[Email], sc: ScioContext)(features: SCollection[FeaturesRow]): SCollection[FeaturesRow] = {
    val titles = sc.textFile("../data/synonym_job_titles_for_index.txt")
      .map(_.split("=>").head)
      .flatMap(_.split(",").map(_.trim)).filter(_.nonEmpty)

    val titlesById = extractTitleFromSignature(emails, titles).debug().asMapSideInput
    features
      .withSideInputs(titlesById)
      .map {
        case (f, ctx) => f.copy(contact_title = ctx(titlesById).getOrElse(f.messageId, ""))
      }.toSCollection
  }

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val originator = args.getOrElse("originator", "vince.kaminski@enron.com")
    implicit val parser: (String => MimeMessage) = MailUtils.parseEmail(encoded = false)(_)

    val emails = EmailReader.readSample(sc, args("input"))
      .map(Parser.parseEmail).flatten
      .timestampBy(_.date).withFixedWindows(Duration.standardDays(1))

    //val unique = emails.groupBy(x => (x.date, x.subject)).mapValues(_.head).values

    val inbounds = emails.filter(_.from != originator)

    val prepareFeatures: (SCollection[Email] => SCollection[FeaturesRow]) =
      simpleFeatures(originator) _ andThen
        withAnsweredFeatures(emails, originator) andThen
        withThreadFeatures(emails, originator) andThen
        withTitle(inbounds, sc)

    val features = prepareFeatures(inbounds)
    features.flatMap(FeaturesRow.unapply).map(_.productIterator.toList.mkString("\t")).saveAsTextFile(args("output"))

    sc.close()

    //res.waitForResult().value.foreach(println)
  }
}
