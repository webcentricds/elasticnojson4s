package com.charter.sia.es

import com.charter.sia.helper.EsMap
import com.sksamuel.elastic4s.TcpClient
import com.sksamuel.elastic4s.bulk.RichBulkResponse
import com.sksamuel.elastic4s.index.RichIndexResponse
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import com.sksamuel.elastic4s.searches.sort.SortDefinition
import com.sksamuel.elastic4s.update.RichUpdateResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.index.reindex.BulkByScrollResponse

import scala.concurrent.Future

class EsClientMapsIdxTyp(val indexName: String,
                         val indexType: String,
                         val esClient: EsClientMaps) {

  def this(tcpClient: TcpClient,
           indexName: String,
           indexType: String) = this(indexName, indexType, new EsClientMaps(tcpClient))

  @inline def countIndex: Long = esClient.countIndex(indexName)
  @inline def countIndexType: Long = esClient.countIndexType(indexName, indexType)
  @inline def countQuery(queryDefinition: QueryDefinition): Long = esClient.countQuery(indexName, indexType, queryDefinition)

  @inline def insertMap(map: Map[String, AnyRef]): RichIndexResponse = esClient.insertMap(indexName, indexType, map)
  @inline def insertMaps(maps: Iterator[Map[String, AnyRef]]): Future[RichBulkResponse] = esClient.insertMaps(indexName, indexType, maps)
  @inline def insertEsMap(esMap: EsMap): Future[RichIndexResponse] = esClient.insertEsMap(indexName, indexType, esMap)
  @inline def insertEsMaps(esMaps: Iterator[EsMap]): Future[RichBulkResponse] = esClient.insertEsMaps(indexName, indexType, esMaps)

  @inline def updateEsMap(esMap: EsMap): Future[RichUpdateResponse] = esClient.updateEsMap(indexName, indexType, esMap)
  @inline def updateEsMaps(esMaps: Iterator[EsMap]): Future[RichBulkResponse] = esClient.updateEsMaps(indexName, indexType, esMaps)

  @inline def upsertEsMap(esMap: EsMap): Future[RichUpdateResponse] = esClient.upsertEsMap(indexName, indexType, esMap)
  @inline def upsertEsMaps(esMaps: Iterator[EsMap]): Future[RichBulkResponse] = esClient.upsertEsMaps(indexName, indexType, esMaps)

  @inline def notifyEsMapsByIds(consumer: (EsMap) => Unit,
                                ids: Seq[String],
                                numReturned: Long = EsClientMaps.defaultNumReturned): Unit =
    esClient.notifyEsMapsByIds(indexName, indexType, consumer, ids, numReturned)
  @inline def notifyEsMaps(consumer: (EsMap) => Unit,
                           queryDefinition: Option[QueryDefinition] = EsClientMaps.defaultQueryDefinition,
                           numReturned: Long = EsClientMaps.defaultNumReturned): Unit =
    esClient.notifyEsMaps(indexName, indexType, consumer, queryDefinition, numReturned)

  @inline def getEsMap(id: String): Option[EsMap] = esClient.getEsMap(indexName, indexType, id)
  @inline def getEsMapsAll(numReturned: Long = EsClientMaps.defaultNumReturned): Iterator[EsMap] =
    esClient.getEsMapsAll(indexName, indexType, numReturned)
  @inline def getEsMapsByIds(ids: Seq[String],
                             numReturned: Long = EsClientMaps.defaultNumReturned): Iterator[EsMap] =
    esClient.getEsMapsByIds(indexName, indexType, ids, numReturned)
  @inline def getEsMaps(queryDefinition: Option[QueryDefinition] = EsClientMaps.defaultQueryDefinition,
                        numReturned: Long = EsClientMaps.defaultNumReturned): Iterator[EsMap] =
    esClient.getEsMaps(indexName, indexType, queryDefinition, numReturned)

  @inline def deleteByIds(ids: Seq[String]): BulkByScrollResponse = esClient.deleteByIds(indexName, indexType, ids)
  @inline def deleteByQuery(queryDefinition: Option[QueryDefinition]): BulkByScrollResponse = esClient.deleteByQuery(indexName, indexType, queryDefinition)
  @inline def deleteIndexType(): BulkByScrollResponse = esClient.deleteIndexType(indexName, indexType)

  @inline def scan(consumer: (EsMap) => Unit,
                   quitAfter: Long = 0,
                   queryDefinition: Option[QueryDefinition] = EsClientMaps.defaultQueryDefinition,
                   storedFields: Option[Seq[String]] = EsClientMaps.defaultStoredFields,
                   sorts: Option[Iterable[SortDefinition]] = EsClientMaps.defaultSorts,
                   scrollTimeout: String = EsClientMaps.defaultScrollTimeout,
                   scrollFetchSize: Int = EsClientMaps.defaultScrollFetchSize,
                   searchType: SearchType = EsClientMaps.defaultSearchType): Unit =
    esClient.scan(indexName,
                  indexType,
                  consumer,
                  quitAfter,
                  queryDefinition,
                  storedFields,
                  sorts,
                  scrollTimeout,
                  scrollFetchSize,
                  searchType)
}
