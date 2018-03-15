import java.nio.file.{Files, Path, Paths}

import Features.FeaturesRow
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.coders.{KvCoder, StringUtf8Coder}
import org.apache.beam.sdk.state.{StateSpec, StateSpecs, ValueState}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, StartBundle, StateId}
import org.apache.beam.sdk.values.KV
import org.tensorflow.{Graph, SavedModelBundle, Session, Tensor}
import shapeless.datatype.tensorflow.TensorFlowType
import shapeless.datatype.tensorflow._

sealed abstract class ModelOrPoint

final case class Model(source: String) extends ModelOrPoint
final case class Point(point: FeaturesRow) extends ModelOrPoint



class Scoring extends DoFn[KV[String, ModelOrPoint], FeaturesRow]{
  //private var model: Option[SavedModelBundle] = None
  val featuresType = TensorFlowType[FeaturesRow]

  @StateId("currentMode") private val currentModel: StateSpec[ValueState[Array[Byte]]] =
    StateSpecs.value()

  @ProcessElement
  def process(c: DoFn[KV[String, ModelOrPoint], FeaturesRow]#ProcessContext,
              @StateId("currentMode") currentModel: ValueState[Array[Byte]]): Unit = {

    c.element.getValue match {
      case Model(source) =>
        println("loading model")

        val raw = Files.readAllBytes(Paths.get(source))
        val graph = new Graph()
        graph.importGraphDef(raw)

        currentModel.write(graph.toGraphDef)
      case Point(point) =>
        Option(currentModel.read()) match
        {
          case Some(graphDef) =>
            val graph = new Graph()
            graph.importGraphDef(graphDef)

            val session = new Session(graph)

            val out = graph.operation("linear/head/predictions/str_classes")
            val in = Tensor.create(Array(featuresType.toExample(point).toByteArray), classOf[String])

            val res = session.runner().feed("input_example_tensor", in).fetch(out.output(0)).run()
              .get(0).expect(classOf[String])

            val nlabels = Array.ofDim[Byte](1, 1, 1)
            c.output(point.copy(answered=res.copyTo(nlabels).head.head.map(_.toChar).mkString == "1"))
          case _ =>
            println("no model loaded")
        }
    }
  }
}


object Scoring {
  def predict(input: SCollection[ModelOrPoint]): SCollection[FeaturesRow] = {
    val fun = new Scoring()
    val kvCoder = KvCoder.of(StringUtf8Coder.of(), input.internal.getCoder)
    input.map(x => KV.of("", x)).setCoder(kvCoder).applyTransform(ParDo.of(fun))
  }
}