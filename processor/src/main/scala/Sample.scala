import javax.mail.internet.MimeMessage

import com.spotify.scio.ContextAndArgs
import utils.{EmailReader, MailUtils, Parser}


object Sample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val emails = EmailReader.readMbox(sc, args("input"))

    implicit val parser: (String => MimeMessage) = MailUtils.parseEmail(encoded = false)(_)
    val originator = args.getOrElse("originator", "moskalenko.alexey@gmail.com")

    val outbound = emails.filter(
      e => {
        val email = Parser.parseEmail(e)
        email.exists(x => x.from == originator && x.inReplyTo.exists(_.nonEmpty))
      }
    ).sample(withReplacement=false, fraction = 0.3)

    val inbound = emails.filter(
      e => {
        val email = Parser.parseEmail(e)
        email.exists(_.from != originator)
      }
    ).sample(withReplacement=false, fraction = 0.1)

//
//    val unique = emails.groupBy(x => {
//      val email = Parser.parseEmail(x).get
//      (email.date, email.subject)
//    }).mapValues(_.head).values

    (inbound ++ outbound).saveAsTextFile(args("output"))
    sc.close()
  }

}