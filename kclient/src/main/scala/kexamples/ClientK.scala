/**
 * https://github.com/AdrianRaFo
 */
package kexamples

object ClientK extends App {
  val producerK = new ProducerK()
  val streamK   = new StreamK()
  val consumerK = new ConsumerK()
  while (true) {
    producerK.send()
    consumerK.receive()
  }
}
