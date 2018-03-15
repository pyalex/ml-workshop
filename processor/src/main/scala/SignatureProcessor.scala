
import javax.mail.internet.MimeMessage

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.TextIO
import utils.Data.Email
import utils.{EmailReader, MailUtils, Parser}

import scala.util.Try


object SignatureProcessor {
  private val signatureStart = Array("thanks,", "regards", "cheers", "best")
  private val signatureEnd = Array("----",
    "from:", "to:", "cc:",
    "ps:", "p.s.", "ps ",
    ">", raw"(\d{2})/(\d{2})/(\d{4})"
  ).map(_.r)

  private val minSignatureLines = 3
  private val maxSignatureLines = 10

  def tokenize(line: String): Array[String] = {
    line.trim.toLowerCase.replaceAll("[^ a-z]", "").split(" ").filter(_.trim.nonEmpty)
  }

  def allNGrams(ns: Array[Int])(line: String): Seq[String] = {
    ns.flatMap(
      n => {
        val tokens = tokenize(line)
        if (tokens.length >= n) tokens.sliding(n).map(_.mkString(" ")).toSeq else Seq()
      })
  }

  def mostFrequent(in: SCollection[(String, String)]): SCollection[(String, String)] = {
    val create = { title: String => Map(title -> 1).withDefaultValue(0) }
    val mergeValue = { (c: Map[String, Int], title: String) => c + (title -> (c(title) + 1)) }
    val mergeCombiners = {
      (c1: Map[String, Int], c2: Map[String, Int]) => (c1.toSeq ++ c2.toSeq).groupBy(_._1).mapValues(_.map(_._2).sum)
    }

    in.combineByKey(create)(mergeValue)(mergeCombiners).mapValues(_.maxBy(_._2)._1)
  }

  def splitBodyBySignature(body: String): (Array[String], Array[String]) = {
    val text = body.split("\n").takeWhile(line => !signatureStart.exists(line.trim.toLowerCase.startsWith(_)))
    val signature = body.split("\n")
      .dropWhile(line => !signatureStart.exists(line.trim.toLowerCase.startsWith(_)))
      .takeWhile(line => !signatureEnd.exists(_.findPrefixMatchOf(line.trim.toLowerCase()).nonEmpty))
      .filter(l => l.trim.nonEmpty && l.length < 50).drop(1)

    (text, signature)
  }

  def extractTitleFromSignature(input: SCollection[Email], titles: SCollection[String]): SCollection[(String, String)] = {
    val candidates = input
      .withGlobalWindow()
      .map(e => (e.messageId, splitBodyBySignature(e.body)))
      .filter { case (_, (_, signature)) => signature.length >= minSignatureLines && signature.length < maxSignatureLines }
      .flatMapValues(_._2.toSeq)
        .debug()
      .flatMapValues(allNGrams(Array(1, 2, 3))).swap

    mostFrequent(candidates.join(titles.map(t => (t.toLowerCase, t.capitalize))).values)
  }


  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    implicit val parser: (String => MimeMessage) = MailUtils.parseEmail(encoded = false)(_)

    val emails = EmailReader.readSample(sc, args("input"))
      .map(Parser.parseEmail).flatten

    val titles = sc.bigQuerySelect("select FindPhrase, ReplacePhrase from workshop.titles where FindPhraseStatus = 'assignedrole'")
      .map(r => r.getStringOpt("ReplacePhrase").getOrElse(r.getString("FindPhrase")))

    extractTitleFromSignature(emails, titles).debug()
    sc.close()
  }
}
