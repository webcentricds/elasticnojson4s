package com.charter.sia.iterator

import com.charter.sia.helper.{EsMap, EsObj, MapObj}

class MapIterator[T](val iterator: Iterator[EsObj[T]]) extends Iterator[EsMap] {
  val esMapClazz: Class[EsMap] = classOf[EsMap]
  @inline override def hasNext: Boolean = iterator.hasNext
  @inline override def next(): EsMap = {
    val esObj: EsObj[T] = iterator.next()
    new EsMap(esObj.id, MapObj.obj2Map(esObj.data))
  }
}
