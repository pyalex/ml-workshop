package legacy

import java.nio.file.{Files, Paths}

import org.tensorflow.{Graph, Session, Tensor}

object TensorTest {
  def main(s: Array[String]): Unit = {
//    val b = SavedModelBundle.load("/Users/oleksiimo/projects/public/data/saved_model/1521025043", "serve")
//    val out = b.graph().operation("linear/head/predictions/str_classes")
//
//    val f = FeaturesRow()
//    val featuresType = TensorFlowType[FeaturesRow]
//
//    val in = Tensor.create(Array(featuresType.toExample(f).toByteArray), classOf[String])
//
//    val res = b.session().runner().feed("input_example_tensor", in).fetch(out.output(0)).run()
//      .get(0).expect(classOf[String])
//
//    val nlabels = Array.ofDim[Byte](1, 1, 1)
//    //val nlabels = rshape(1).asInstanceOf[Int]
//    val fin = (res.copyTo(nlabels)(0)(0)).map(_.toChar).mkString
//    print(fin)

    val raw = Files.readAllBytes(Paths.get("/Users/oleksiimo/projects/public/practical_seq2seq/graph.pb"))
    val g = new Graph()
    g.importGraphDef(raw)

    val session = new Session(g)
    val out_prefix = "decoder/embedding_rnn_seq2seq_1/embedding_rnn_decoder/rnn_decoder/OutputProjectionWrapper"

    val encoded = Array.fill(20)(0)
    encoded(0) = 130
    encoded(1) = 7
    encoded(2) = 93
    encoded(3) = 31

    var res = session.runner()
    for (i <- 0 to 19) {
      res = res.feed("ei_" + i + ":0", Tensor.create(Array(encoded(i).toLong), classOf[java.lang.Long]))
    }

    res = res.feed("Placeholder", Tensor.create(new java.lang.Float(1.0), classOf[java.lang.Float]))
    res = res.fetch(out_prefix + "/add")
    for (i <- 1 to 19) {
      res = res.fetch(out_prefix + "_" + i + "/add")
    }

    val r = res.run()
    val outcome = Array.fill(20)(0)

    for (i <- 0 to 19) {
      val row = r.get(i)
      val dst = Array.fill(1, row.shape()(1).toInt)(0.toFloat)
      outcome(i) = row.copyTo(dst)(0).zipWithIndex.maxBy(_._1)._2
    }

    println(outcome.mkString(" "))
  }
}
