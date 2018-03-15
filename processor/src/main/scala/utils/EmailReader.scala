package utils

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.TextIO

object EmailReader {
  def readSample(sc: ScioContext, filePath: String): SCollection[String] = {
    val reader = TextIO.read().from(filePath).withDelimiter("\nMessage-ID:".getBytes)

    sc.wrap(sc.pipeline.apply(reader))
      .map("Message-ID:" ++ _.trim)
  }

  def readEnron(sc: ScioContext, filePath: String): SCollection[String] = {
    val reader = TextIO.read().from(filePath).withDelimiter("\"Message-ID:".getBytes)
    sc.wrap(sc.pipeline.apply(reader))
      .map(_.split("\n").dropRight(1).mkString("\n")) // drop last line
      .filter(_.length > 0)
      .map("Message-ID:" ++ _.trim)
  }

  def readMbox(sc: ScioContext, filePath: String): SCollection[String] = {
    val reader = TextIO.read().from(filePath).withDelimiter("\nFrom ".getBytes)
    sc.wrap(sc.pipeline.apply(reader))
  }
}
