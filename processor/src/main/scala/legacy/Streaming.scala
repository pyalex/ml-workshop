package legacy

import com.spotify.scio.ScioContext
import com.spotify.scio.streaming.ACCUMULATING_FIRED_PANES
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.beam.sdk.options.{PipelineOptions, StreamingOptions}
import org.apache.beam.sdk.transforms.windowing.{AfterProcessingTime, Repeatedly}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Duration, Instant}
import utils.Data._
import utils.Parser._
import utils._


object Streaming {

  private val token = "ya29.Glx_BQAmh3LNdGumViGiYNWWCqTNStgc_9Lq1Nexv-eqVmA8N6Ig-YXUGvQgICHt_PSc5gZSi5rHKXhkTp" +
    "-qOfadrEtxVmKgKsAPxlJ0vS0nSYu9_C8wa_EH5YyCOg"
  private val gmailClient = new GmailClient(token)

//  private val logger = Logger.getLogger(classOf[HttpTransport].getName)
//  logger.setLevel(Level.CONFIG)
//
//  private val handler = new ConsoleHandler
//  handler.setLevel(Level.CONFIG)
//  logger.addHandler(handler)


  def toEmails(ids: Set[MessageKey]): Seq[Email] = {
    ids.flatMap{
      key => gmailClient.getMessage(key.messageId)
        .flatMap(x => parseEmail(x.getRaw))
        .map(_.copy(threadId = Some(key.threadId)))
    }.toSeq
  }

  def parseDate(date: String): Instant = {
    val format = DateTimeFormat.forPattern("EEE, d MMM yyyy HH:mm:ss Z")
    DateTime.parse(date.trim.take(31), format).toInstant
  }

  def withThreadLength(emails: SCollection[Email]): SCollection[(String, Int)] = {
//    val windowed = emails.windowByDays(1, options = WindowOptions(
//      allowedLateness = Duration.ZERO,
//      trigger = Repeatedly.forever(
//      AfterProcessingTime
//        .pastFirstElementInPane()
//        .plusDelayOf(Duration.standardMinutes(1)))
//      .orFinally(
//        AfterWatermark.pastEndOfWindow()),
//      //trigger= Repeatedly.forever(AfterWatermark.pastEndOfWindow()),
//      accumulationMode = ACCUMULATING_FIRED_PANES)
//    )

    val windowed = emails.withGlobalWindow(options = WindowOptions(
            allowedLateness = Duration.ZERO,
            accumulationMode = ACCUMULATING_FIRED_PANES,
            trigger = Repeatedly.forever(
            AfterProcessingTime
              .pastFirstElementInPane()
              .plusDelayOf(Duration.standardMinutes(1)))))

    windowed.map(x => (x.threadId.getOrElse(""), 1)).sumByKey
  }

  def withThreadAnswer(emails: SCollection[Email], originator: String): SCollection[String] = {
    val windowed = emails.withGlobalWindow(options = WindowOptions(
      allowedLateness = Duration.ZERO,
      accumulationMode = ACCUMULATING_FIRED_PANES,
      trigger = Repeatedly.forever(
        AfterProcessingTime
          .pastFirstElementInPane()
          .plusDelayOf(Duration.standardSeconds(1)))))

    windowed.filter(_.from == originator).flatMap(_.threadId).distinct
  }

  def main(cmdLineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[PipelineOptions](cmdLineArgs)
    opts.as(classOf[StreamingOptions]).setStreaming(true)

    val sc = ScioContext(opts)
    val originator = args.getOrElse("originator", "oleksiimo@wix.com")

    val notifications = sc.pubsubTopic(args("input"))
      .map(JsonUtil.fromJson[PushNotification])

    val emails = HistoryLoader.getMessageIds(notifications, token, "me").debug().flatMap(toEmails)
    //emails.saveAsTypedBigQuery(args("output"), WriteDisposition.WRITE_APPEND)
    val features = Features.simpleFeatures(originator)(emails)

    val lengthByThread = withThreadLength(emails).asMultiMapSideInput
//    val answeredThreads = withThreadAnswer(emails, originator).asListSideInput
//
    emails.withFixedWindows(Duration.standardSeconds(1)).withSideInputs(lengthByThread).map {
      case (e, ctx) => (e.threadId, e.subject, ctx(lengthByThread).get(e.threadId.getOrElse("")))
    }.toSCollection.debug()

//    emails.withFixedWindows(Duration.standardSeconds(1)).withSideInputs(answeredThreads).map {
//      case (e, ctx) => (e.subject, ctx(answeredThreads).contains(e.thread.getOrElse("")))
//    }.toSCollection.debug()

    val models: SCollection[ModelOrPoint] = sc.pubsubTopic(args("model_input")).map(Model)
    val modelsAndFeatures: SCollection[ModelOrPoint] = features.map(Point)

    Scoring.predict(modelsAndFeatures ++ models).debug()

    val result = sc.close()
    result.waitUntilDone()
  }
}
