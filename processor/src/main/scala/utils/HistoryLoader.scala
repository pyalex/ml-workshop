package utils

import com.google.common.base.MoreObjects.firstNonNull
import org.apache.beam.sdk.coders.{Coder, KvCoder, StringUtf8Coder, VarLongCoder}
import org.apache.beam.sdk.state.{StateSpec, StateSpecs, ValueState}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, StartBundle, StateId}
import org.apache.beam.sdk.values.KV
import Data._

import com.spotify.scio.values.SCollection

class HistoryLoader(val userToken: String, val userId: String) extends DoFn[KV[String, PushNotification], Set[MessageKey]] {
  private var gmailClient: GmailClient = _

  @StateId("currentHistoryId") private val historyIdState: StateSpec[ValueState[Long]] =
    StateSpecs.value(VarLongCoder.of().asInstanceOf[Coder[Long]])

  @StartBundle
  def startBundle(c: DoFn[KV[String, PushNotification], Set[MessageKey]]#StartBundleContext): Unit =
    gmailClient = new GmailClient(userToken, userId)

  @ProcessElement
  def process(c: DoFn[KV[String, PushNotification], Set[MessageKey]]#ProcessContext,
              @StateId("currentHistoryId") historyIdState: ValueState[Long]): Unit = {
    val historyId = firstNonNull(historyIdState.read, 0L)
    if (historyId > 0) c.output(gmailClient.listHistory(historyId).getOrElse(Set.empty[MessageKey]))
    historyIdState.write(c.element.getValue.historyId)
  }
}

object HistoryLoader {
  def getMessageIds(input: SCollection[PushNotification], userToken: String, userId: String): SCollection[Set[MessageKey]] = {
    val fun = new HistoryLoader(userToken, userId)
    val kvCoder = KvCoder.of(StringUtf8Coder.of(), input.internal.getCoder)
    input.map(x => KV.of(x.emailAddress, x)).setCoder(kvCoder).applyTransform(ParDo.of(fun))
  }
}