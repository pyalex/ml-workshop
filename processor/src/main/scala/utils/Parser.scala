package utils

import javax.mail.internet.{InternetAddress, MimeMessage}

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.{DateTime, Instant, LocalDateTime}
import utils.Data.Email

import scala.util.Try

object Parser {
  def parseAddressHeader(email: MimeMessage, header: String): List[String] =
    Option(email.getHeader(header, ","))
      .map(InternetAddress.parseHeader(_, false))
      .map(_.map(_.getAddress).toList)
      .getOrElse(List.empty)

  def parseDate(date: String)(implicit formats: Array[DateTimeFormatter]): Instant = {
    formats.flatMap(
      x => Try(DateTime.parse(date.trim.take(31), x)
      ).map(_.toInstant).toOption).head
  }

  def normalizeSubject(subject: String): String = {
    subject.toLowerCase.replaceAll("re:", "").trim
  }

  def parseEmail(email: String)(implicit parser: (String => MimeMessage)): Option[Email] = {
    Try(parser(email)).map(mimeMessage =>
      Email(
        mimeMessage.getHeader("Message-ID", null),
        parseDate(mimeMessage.getHeader("Date", null)),
        Option(mimeMessage.getHeader("Subject", null)).getOrElse(""),
        Parser.parseAddressHeader(mimeMessage, "From").head,
        Parser.parseAddressHeader(mimeMessage, "To"),
        Parser.parseAddressHeader(mimeMessage, "Cc"),
        "",//MailUtils.getMessageBody(mimeMessage),
        thread = Option(mimeMessage.getHeader("X-GM-THRID", null)),
        inReplyTo = Option(mimeMessage.getHeader("In-Reply-To", null))
      )
    ).toOption
  }

  implicit val formats: Array[DateTimeFormatter] = Array(
    DateTimeFormat.forPattern("EEE, d MMM yyyy HH:mm:ss Z"),
    DateTimeFormat.forPattern("EEE, d MMM yyyy HH:mm:ss")
  )

  implicit val emailParser: (String => MimeMessage) = MailUtils.parseEmail(encoded = true)(_)
}
