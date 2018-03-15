package utils

import com.spotify.scio.bigquery.BigQueryType
import org.joda.time.Instant

object Data {
  case class PushNotification(emailAddress: String, historyId: Long)

  @BigQueryType.toTable
  case class Email(messageId: String, date: Instant, subject: String,
                   from: String, to: List[String], cc: List[String],
                   body: String,
                   inReplyTo: Option[String] = None, thread: Option[String] = None)
}
