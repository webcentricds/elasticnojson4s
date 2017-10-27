package com.charter.sia.es

import com.charter.sia.helper.{EsMap, EsObj, MapObj}
import com.charter.sia.iterator.{MapIterator, ObjIterator}
import com.sksamuel.elastic4s.TcpClient
import com.sksamuel.elastic4s.bulk.RichBulkResponse
import com.sksamuel.elastic4s.index.RichIndexResponse
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import com.sksamuel.elastic4s.update.RichUpdateResponse
import com.sksamuel.exts.concurrent.Futures.RichFuture
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.index.reindex.BulkByScrollResponse
import org.slf4j.LoggerFactory

import scala.concurrent.Future


class EsClient[T](val esClientMapsIdxTyp: EsClientMapsIdxTyp,
                  val tClazz: Class[T]) {
  private val LOG = LoggerFactory.getLogger(classOf[EsClientMaps])

  def this(tcpClient: TcpClient,
           indexName: String,
           indexType: String,
           tClazz: Class[T]) =
    this(new EsClientMapsIdxTyp(indexName, indexType, new EsClientMaps(tcpClient)),
         tClazz)

  @inline def countIndex: Long =
    esClientMapsIdxTyp.countIndex
  @inline def countIndexType: Long =
    esClientMapsIdxTyp.countIndexType
  @inline def countQuery(queryDefinition: QueryDefinition): Long =
    esClientMapsIdxTyp.countQuery(queryDefinition)

  @inline def insertEsObj(esObj: EsObj[T]): RichFuture[RichIndexResponse] =
    esClientMapsIdxTyp.insertEsMap(new EsMap(esObj.id, MapObj.obj2Map(esObj.data)))
  @inline def insertEsObjs(esObjs: Iterator[EsObj[T]]): Future[RichBulkResponse] =
    esClientMapsIdxTyp.insertEsMaps(new MapIterator[T](esObjs))

  @inline def updateEsObj(esObj: EsObj[T]): Future[RichUpdateResponse] =
    esClientMapsIdxTyp.updateEsMap(new EsMap(esObj.id, MapObj.obj2Map(esObj.data)))
  @inline def updateEsObjs(esObjs: Iterator[EsObj[T]]): Future[RichBulkResponse] =
    esClientMapsIdxTyp.updateEsMaps(new MapIterator[T](esObjs))

  @inline def upsertEsObj(esObj: EsObj[T]): Future[RichUpdateResponse] =
    esClientMapsIdxTyp.upsertEsMap(new EsMap(esObj.id, MapObj.obj2Map(esObj.data)))
  @inline def upsertEsObjs(esObjs: Iterator[EsObj[T]]): Future[RichBulkResponse] =
    esClientMapsIdxTyp.upsertEsMaps(new MapIterator[T](esObjs))

  @inline def notifyEsObjsByIds(consumer: (EsObj[T]) => Unit,
                                ids: Seq[String],
                                numReturned: Long = EsClientMaps.defaultNumReturned): Unit = {
    val esMapConsumer = (esMap: EsMap) => consumer(new EsObj[T](esMap.id, MapObj.map2Obj(esMap.map, tClazz)))
    esClientMapsIdxTyp.notifyEsMapsByIds(esMapConsumer, ids, numReturned)
  }
  @inline def notifyEsObjs(consumer: (EsObj[T]) => Unit,
                           queryDefinition: Option[QueryDefinition] = EsClientMaps.defaultQueryDefinition,
                           numReturned: Long = EsClientMaps.defaultNumReturned): Unit = {
    val esMapConsumer = (esMap: EsMap) => consumer(new EsObj[T](esMap.id, MapObj.map2Obj(esMap.map, tClazz)))
    esClientMapsIdxTyp.notifyEsMaps(esMapConsumer, queryDefinition, numReturned)
  }

  @inline def getEsObj(id: String): Option[EsObj[T]] = {
    val esMapOption = esClientMapsIdxTyp.getEsMap(id)
    if(esMapOption.isEmpty)
      None
    else Option(new EsObj(esMapOption.get.id, MapObj.map2Obj(esMapOption.get.map, tClazz)))
  }
  @inline def getEsObjsAll(numReturned: Long = EsClientMaps.defaultNumReturned): Iterator[EsObj[T]] =
    new ObjIterator[T](esClientMapsIdxTyp.getEsMapsAll(numReturned), tClazz)
  @inline def getEsObjsByIds(ids: Seq[String],
                             numReturned: Long = EsClientMaps.defaultNumReturned): Iterator[EsObj[T]] =
    new ObjIterator[T](esClientMapsIdxTyp.getEsMapsByIds(ids, numReturned), tClazz)
  @inline def getEsObjsByQuery(queryDefinition: QueryDefinition,
                               numReturned: Long = EsClientMaps.defaultNumReturned): Iterator[EsObj[T]] =
    new ObjIterator[T](esClientMapsIdxTyp.getEsMaps(Option(queryDefinition), numReturned), tClazz)

  @inline def deleteByIds(ids: Seq[String]): BulkByScrollResponse =
    esClientMapsIdxTyp.deleteByIds(ids)
  @inline def deleteByQuery(queryDefinition: Option[QueryDefinition]): BulkByScrollResponse =
    esClientMapsIdxTyp.deleteByQuery(queryDefinition)
  @inline def deleteIndexType(): BulkByScrollResponse =
    esClientMapsIdxTyp.deleteIndexType()

  @inline def scan(consumer: (EsObj[T]) => Unit,
                   quitAfter: Long = 0,
                   queryDefinition: Option[QueryDefinition] = EsClientMaps.defaultQueryDefinition,
                   storedFields: Option[Seq[String]] = EsClientMaps.defaultStoredFields,
                   scrollTimeout: String = EsClientMaps.defaultScrollTimeout,
                   scrollFetchSize: Int = EsClientMaps.defaultScrollFetchSize,
                   searchType: SearchType = EsClientMaps.defaultSearchType): Unit = {
    val esMapConsumer = (esMap: EsMap) => consumer(new EsObj[T](esMap.id, MapObj.map2Obj(esMap.map, tClazz)))
    esClientMapsIdxTyp.scan(esMapConsumer,
                            quitAfter,
                            queryDefinition,
                            storedFields,
                            scrollTimeout,
                            scrollFetchSize,
                            searchType)
  }
}
