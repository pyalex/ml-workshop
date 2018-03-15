import Features.FeaturesRow
import org.tensorflow.{SavedModelBundle, Tensor}
import shapeless.datatype.tensorflow.TensorFlowType

import shapeless.datatype.tensorflow._
import com.spotify.scio.tensorflow._

object TensorTest {
  def main(s: Array[String]): Unit = {
    val b = SavedModelBundle.load("/Users/oleksiimo/projects/public/data/saved_model/1521025043", "serve")
    val out = b.graph().operation("linear/head/predictions/str_classes")

    val f = FeaturesRow()
    val featuresType = TensorFlowType[FeaturesRow]

    val in = Tensor.create(Array(featuresType.toExample(f).toByteArray), classOf[String])

    val res = b.session().runner().feed("input_example_tensor", in).fetch(out.output(0)).run()
      .get(0).expect(classOf[String])

    val nlabels = Array.ofDim[Byte](1, 1, 1)
    //val nlabels = rshape(1).asInstanceOf[Int]
    val fin = (res.copyTo(nlabels)(0)(0)).map(_.toChar).mkString
    print(fin)
  }
}
