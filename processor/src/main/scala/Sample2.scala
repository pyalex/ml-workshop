import javax.mail.internet.MimeMessage

import com.spotify.scio.ContextAndArgs
import org.apache.beam.sdk.io.TextIO
import utils.Data.Email
import utils.Parser.parseDate
import utils.{MailUtils, Parser}

import scala.util.Try





object Sample2 {

  def main(cmdLineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdLineArgs)
    val myEmail = "moskalenko.alexey@gmail.com"
    implicit val parser: (String => MimeMessage) = MailUtils.parseEmail(encoded = false)(_)

    val reader = TextIO.read()
      .from(args("input"))
      .withDelimiter("\nFrom ".getBytes)

    val raw = sc.wrap(sc.pipeline.apply(reader))

    val inbounds = raw.filter( x => {
      val email = Parser.parseEmail(x).get
      email.from != myEmail
    })

    val outbounds = raw.filter( x => {
      val email = Parser.parseEmail(x).get
      email.from == myEmail
    })


    val output = inbounds.sample(withReplacement = false, 0.1) ++
      outbounds.sample(withReplacement = false, 0.5)

    output.saveAsTextFile(args("output"))

    sc.close()

    //res.waitForResult().value.foreach(println)

  }
}
