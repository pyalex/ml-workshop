package legacy

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import javax.mail.{Header, Session}
import javax.mail.internet.MimeMessage

import com.spotify.scio.ContextAndArgs
import utils.{EmailReader, MailUtils, Parser}

import scala.util.Try


object Sample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val emails = EmailReader.readMbox(sc, args("input"))

    implicit val parser: (String => MimeMessage) = MailUtils.parseEmail(encoded = false)(_)
    val originator = args.getOrElse("originator", "moskalenko.alexey@gmail.com")

    val withoutBody = emails.flatMap(
      e => {
        Try(parser(e)).map(
          mime => {
            val n = new MimeMessage(mime)
            val enum = mime.getAllHeaders
            while (enum.hasMoreElements) {
              val h = enum.nextElement().asInstanceOf[Header]
              n.setHeader(h.getName, h.getValue)
            }
            val s = new ByteArrayOutputStream()
            n.writeTo(s)
            new String(s.toByteArray)
          }).toOption
      }
    )

    withoutBody.saveAsTextFile(args("output"))

//      .sample(withReplacement=false, fraction = 0.3)
//
//    val inbound = emails.filter(
//      e => {
//        val email = Parser.parseEmail(e)
//        email.exists(_.from != originator)
//      }
//    ).sample(withReplacement=false, fraction = 0.1)

//
//    val unique = emails.groupBy(x => {
//      val email = Parser.parseEmail(x).get
//      (email.date, email.subject)
//    }).mapValues(_.head).values

//    (inbound ++ outbound).saveAsTextFile(args("output"))
    sc.close()
  }

}