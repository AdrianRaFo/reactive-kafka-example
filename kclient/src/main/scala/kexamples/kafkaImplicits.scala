/**
 * https://github.com/AdrianRaFo
 */
package kexamples

import org.apache.kafka.streams.KeyValue

trait Implicits {
  implicit def Tuple2ToKeyValue[K, V](tuple: (K, V)): KeyValue[K, V] =
    new KeyValue(tuple._1, tuple._2)
}

object kafkaImplicits extends Implicits
