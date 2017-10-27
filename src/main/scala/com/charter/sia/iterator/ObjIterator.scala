package com.charter.sia.iterator

import com.charter.sia.helper.{EsMap, EsObj, MapObj}

class ObjIterator[T](val iterator:Iterator[EsMap],
                     val tClazz: Class[T]) extends Iterator[EsObj[T]] {
  override def hasNext: Boolean = iterator.hasNext
  override def next(): EsObj[T] = {
    val esMap: EsMap = iterator.next()
    new EsObj(esMap.id, MapObj.map2Obj(esMap.map, tClazz))
  }
}
