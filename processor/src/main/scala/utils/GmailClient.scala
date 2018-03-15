package utils

import java.io.ByteArrayInputStream
import java.util.Properties
import javax.mail.Session
import javax.mail.internet.{MimeMessage, MimeMultipart}

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.repackaged.org.apache.commons.codec.binary.Base64
import com.google.api.services.gmail.Gmail
import com.google.api.services.gmail.model.Message
import com.spotify.scio.coders.KryoRegistrar
import de.javakaffee.kryoserializers.jodatime.{JodaDateTimeSerializer, JodaLocalDateSerializer, JodaLocalDateTimeSerializer}
import org.jsoup.Jsoup

import scala.collection.JavaConverters._
import scala.util.{Success, Try}

case class MessageKey(messageId: String, threadId: String)

class GmailClient(userToken: String, userId: String = "me") {
  def getClient: Gmail = {
    val transport = GoogleNetHttpTransport.newTrustedTransport
    val jsonFactory = JacksonFactory.getDefaultInstance
    val credential = new GoogleCredential().setAccessToken(userToken)
    new Gmail.Builder(transport, jsonFactory, credential).setApplicationName("ML Workshop").build()
  }

  def listHistory(start: BigInt): Option[Set[MessageKey]] = {
    val resp = Try(getClient.users().history().list(userId)
      .setStartHistoryId((start - 1).bigInteger).execute)

    resp match {
      case Success(r) =>
        Option(r.getHistory).map(_.asScala.flatMap(
          _.getMessages.asScala.map(x => MessageKey(x.getId, x.getThreadId))).toSet)
      case _ =>
        println("Error: probably not authorized")
        None
    }
  }

  def getMessage(messageId: String): Option[Message] = {
    Try(getClient.users().messages().get(userId, messageId).setFormat("raw").execute).toOption
  }

}


object MailUtils {
  private val b64 = new Base64(true)
  private val props = new Properties()
  private val session = Session.getDefaultInstance(props, null)

  def parseEmail(encoded: Boolean)(email: String): MimeMessage = {
    val bytes = if (encoded) b64.decode(email) else email.getBytes
    new MimeMessage(session, new ByteArrayInputStream(bytes))
  }

  def getMessageBody(mimeMessage: MimeMessage): String = {
    if (mimeMessage.isMimeType("text/plain")) mimeMessage.getContent.toString
    else getMessageBodyFromMultipart(mimeMessage.getContent.asInstanceOf[MimeMultipart])
  }

  def getMessageBodyFromMultipart(mimeMultipart: MimeMultipart): String = {
    val bodyPart = mimeMultipart.getBodyPart(0)
    if (bodyPart.isMimeType("text/plain")) bodyPart.getContent.asInstanceOf[String]
    else if (bodyPart.isMimeType("text/html")) Jsoup.parse(bodyPart.getContent.asInstanceOf[String]).text
    else getMessageBodyFromMultipart(bodyPart.getContent.asInstanceOf[MimeMultipart])
  }
}

import com.twitter.chill._

@KryoRegistrar
class MyKryoRegistrar extends IKryoRegistrar {
  override def apply(k: Kryo): Unit = {
    k.forClass(new JodaDateTimeSerializer)
    k.forClass(new JodaLocalDateSerializer)
    k.forClass(new JodaLocalDateTimeSerializer)
  }
}