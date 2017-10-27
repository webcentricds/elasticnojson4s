package com.charter.sia.es

import java.util.concurrent.atomic.AtomicLong

import com.charter.sia.helper.{EsMap, MapObj}
import com.sksamuel.elastic4s.ElasticDsl.{search, _}
import com.sksamuel.elastic4s.TcpClient
import com.sksamuel.elastic4s.bulk.RichBulkResponse
import com.sksamuel.elastic4s.index.RichIndexResponse
import com.sksamuel.elastic4s.indexes.IndexDefinition
import com.sksamuel.elastic4s.searches.RichSearchResponse
import com.sksamuel.elastic4s.searches.queries.QueryDefinition
import com.sksamuel.elastic4s.update.{RichUpdateResponse, UpdateDefinition}
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.index.reindex.BulkByScrollResponse
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration.{Duration, SECONDS}

object EsClientMaps {
  val defaultScrollTimeout = "2m"
  val defaultQueryDefinition: Option[QueryDefinition] = None
  val defaultStoredFields: Option[Seq[String]] = None
  val defaultScrollFetchSize: Int = 1000
  val defaultSearchType: SearchType = SearchType.DEFAULT
  val defaultNumReturned = 1000
  val awaitDur = Duration(100, SECONDS)
}

class EsClientMaps(val tcpClient: TcpClient) {
  private val LOG = LoggerFactory.getLogger(classOf[EsClientMaps])

  //===========================================================================
  // Count
  //===========================================================================
  def countIndex(indexName: String): Long = tcpClient.execute {
    search(indexName).from(0).size(0)
  }.await(EsClientMaps.awaitDur).totalHits

  def countIndexType(indexName: String,
                     indexType: String): Long = tcpClient.execute {
    search(indexName -> indexType).from(0).size(0)
  }.await(EsClientMaps.awaitDur).totalHits

  def countQuery(indexName: String,
                 indexType: String,
                 queryDefinition: QueryDefinition): Long = tcpClient.execute {
    search(indexName -> indexType).from(0).size(0).query(queryDefinition)
  }.await(EsClientMaps.awaitDur).totalHits

  //===========================================================================
  // Insert
  //===========================================================================
  @inline def insertMap(indexName: String,
                        indexType: String,
                        map: Map[String, AnyRef]): RichIndexResponse = tcpClient.execute {
    index(indexName -> indexType).fields(MapObj.obj2Map(map))
  }.await(EsClientMaps.awaitDur)

  @inline def insertMaps(indexName: String,
                         indexType: String,
                         maps: Iterator[Map[String, AnyRef]]): Future[RichBulkResponse] = {
    val buldOps: Iterator[IndexDefinition] = for(map <- maps)
      yield { indexInto(indexName / indexType).fields(map) }
    tcpClient.execute {
      bulk(buldOps.toList: _*)
    }
  }

  @inline def insertEsMap(indexName: String,
                          indexType: String,
                          esMap: EsMap): Future[RichIndexResponse] = tcpClient.execute {
    index(indexName -> indexType).id(esMap.id).fields(MapObj.obj2Map(esMap.map))
  }

  @inline def insertEsMaps(indexName: String,
                           indexType: String,
                           esMaps: Iterator[EsMap]): Future[RichBulkResponse] = {
    val buldOps: Iterator[IndexDefinition] = for(esMap <- esMaps)
      yield { indexInto(indexName / indexType).id(esMap.id).fields(esMap.map) }
    tcpClient.execute {
      bulk(buldOps.toList: _*)
    }
  }

  //===========================================================================
  // Update
  //===========================================================================
  @inline def updateEsMap(indexName: String,
                          indexType: String,
                          esMap: EsMap): Future[RichUpdateResponse] = tcpClient.execute {
    update(esMap.id).in(indexName/indexType).doc(esMap.map)
  }

  @inline def updateEsMaps(indexName: String,
                           indexType: String,
                           esMaps: Iterator[EsMap]): Future[RichBulkResponse] = {
    val buldOps: Iterator[UpdateDefinition] = for(esMap <- esMaps)
      yield { update(esMap.id).in(indexName/indexType).doc(esMap.map) }
    tcpClient.execute {
      bulk(buldOps.toList: _*)
    }
  }

  //===========================================================================
  // Upsert
  //===========================================================================
  @inline def upsertEsMap(indexName: String,
                          indexType: String,
                          esMap: EsMap): Future[RichUpdateResponse] = tcpClient.execute {
    update(esMap.id).in(indexName/indexType).docAsUpsert(esMap.map)
  }

  @inline def upsertEsMaps(indexName: String,
                           indexType: String,
                           esMaps: Iterator[EsMap]): Future[RichBulkResponse] = {
    val buldOps: Iterator[UpdateDefinition] = for(esMap <- esMaps)
      yield { update(esMap.id).in(indexName/indexType).docAsUpsert(esMap.map) }
    tcpClient.execute {
      bulk(buldOps.toList: _*)
    }
  }

  //===========================================================================
  // Notify
  //===========================================================================
  @inline def notifyEsMapsByIds(indexName: String,
                                indexType: String,
                                consumer: (EsMap) => Unit,
                                ids: Seq[String],
                                numReturned: Long = EsClientMaps.defaultNumReturned): Unit = {
    notifyEsMaps(indexName, indexType, consumer, Option(idsQuery(ids)), numReturned)
  }

  /**
    * Get all the maps in an index / type
    *
    * @param indexName        The name of the index
    * @param indexType        the type of the index
    * @param queryDefinition  An optional query to filter by
    * @param numReturned      An optional number of items returned (quits before gathering all).  Defaults to all
    *
    * @return  A list of the results
    */
  @inline def notifyEsMaps(indexName: String,
                           indexType: String,
                           consumer: (EsMap) => Unit,
                           queryDefinition: Option[QueryDefinition] = EsClientMaps.defaultQueryDefinition,
                           numReturned: Long = EsClientMaps.defaultNumReturned): Unit = {
    scan(indexName,
         indexType,
         consumer,
         numReturned,
         queryDefinition)
  }

  //===========================================================================
  // Get
  //===========================================================================
  @inline def getEsMap(indexName: String,
                       indexType: String,
                       id: String): Option[EsMap] = {
    val richSearchResponse: RichSearchResponse = tcpClient.execute {
      search(indexName / indexType).query(idsQuery(id))
    }.await(EsClientMaps.awaitDur)
    if(richSearchResponse.hits.length == 1) {
      val hit = richSearchResponse.hits(0)
      return Some(new EsMap(hit.id, hit.sourceAsMap))
    }
    None
  }

  @inline def getEsMapsAll(indexName: String,
                           indexType: String,
                           numReturned: Long = EsClientMaps.defaultNumReturned): Iterator[EsMap] = {
    val listBuffer = new ListBuffer[EsMap]
    val consumer = (esMap: EsMap) => listBuffer += esMap : Unit
    scan(indexName,
      indexType,
      consumer,
      numReturned)
    listBuffer.toList.iterator
  }

  @inline def getEsMapsByIds(indexName: String,
                             indexType: String,
                             ids: Seq[String],
                             numReturned: Long = EsClientMaps.defaultNumReturned): Iterator[EsMap] = {
    val listBuffer = new ListBuffer[EsMap]
    val consumer = (esMap: EsMap) => listBuffer += esMap : Unit
    scan(indexName,
         indexType,
         consumer,
         numReturned,
         Option(idsQuery(ids)))
    listBuffer.toList.iterator
  }

  /**
    * Get all the maps in an index / type
    *
    * @param indexName        The name of the index
    * @param indexType        the type of the index
    * @param queryDefinition  An optional query to filter by
    * @param numReturned      An optional number of items returned (quits before gathering all).  Defaults to all
    *
    * @return  A list of the results
    */
  @inline def getEsMaps(indexName: String,
                        indexType: String,
                        queryDefinition: Option[QueryDefinition] = EsClientMaps.defaultQueryDefinition,
                        numReturned: Long = EsClientMaps.defaultNumReturned): Iterator[EsMap] = {
    val listBuffer = new ListBuffer[EsMap]
    val consumer = (esMap: EsMap) => listBuffer += esMap : Unit
    scan(indexName,
      indexType,
      consumer,
      numReturned,
      queryDefinition)
    listBuffer.toList.iterator
  }

  //===========================================================================
  // Delete
  //===========================================================================
  @inline def deleteByIds(indexName: String,
                     indexType: String,
                     ids: Seq[String]): BulkByScrollResponse = {
    deleteByQuery(indexName, indexType, Option(idsQuery(ids)))
  }

  @inline def deleteByQuery(indexName: String,
                            indexType: String,
                            queryDefinition: Option[QueryDefinition]): BulkByScrollResponse = {
    val listBuffer = new ListBuffer[String]
    val consumer = (esMap: EsMap) => listBuffer += esMap.id : Unit
    notifyEsMaps(indexName, indexType, consumer, queryDefinition, 0)
    tcpClient.execute {
      deleteIn(indexName / indexType).by(queryDefinition.get)
    }.await(EsClientMaps.awaitDur)
  }

  @inline def deleteIndexType(indexName: String,
                              indexType: String): BulkByScrollResponse = {
    tcpClient.execute {
      deleteIn(indexName / indexType).by(matchAllQuery())
    }.await(EsClientMaps.awaitDur)
  }

  //===========================================================================
  // Helper Methods
  //===========================================================================
  /**
    * This method is used to gather results given a query.
    *
    * @param indexName        The name of the index
    * @param indexType        The type of the index
    * @param consumer         A lambda that gets called for each result
    * @param quitAfter        An optional short circuit stopping mechanism for query.  Defaults to all
    * @param queryDefinition  An optional query to filter with.  Defaults to none
    * @param storedFields     Optional fields that we will get from documents.  Defaults to all
    * @param scrollTimeout    The scroll timout (ElasticSearch parameter)
    * @param scrollFetchSize  The number of objects to get fetched for each scroll
    * @param searchType       The default search type.
    */
  def scan(indexName: String,
           indexType: String,
           consumer: (EsMap) => Unit,
           quitAfter: Long = 0,
           queryDefinition: Option[QueryDefinition] = EsClientMaps.defaultQueryDefinition,
           storedFields: Option[Seq[String]] = EsClientMaps.defaultStoredFields,
           scrollTimeout: String = EsClientMaps.defaultScrollTimeout,
           scrollFetchSize: Int = EsClientMaps.defaultScrollFetchSize,
           searchType: SearchType = EsClientMaps.defaultSearchType): Unit = {

    //=========================================================================
    // TODO: Find a more elegant way to do this
    //=========================================================================
    val sr = if(queryDefinition.isDefined && storedFields.isDefined)
      search(indexName/indexType).keepAlive(scrollTimeout)
                                 .size(scrollFetchSize)
                                 .searchType(searchType)
                                 .query(queryDefinition.get)
                                 .storedFields(storedFields.get)
    else if(queryDefinition.isDefined)
      search(indexName/indexType).keepAlive(scrollTimeout)
                                 .size(scrollFetchSize)
                                 .searchType(searchType)
                                 .query(queryDefinition.get)
    else if(storedFields.isDefined)
      search(indexName/indexType).keepAlive(scrollTimeout)
                                 .size(scrollFetchSize)
                                 .searchType(searchType)
                                 .storedFields(storedFields.get)
    else
      search(indexName/indexType).keepAlive(scrollTimeout)
                                 .size(scrollFetchSize)
                                 .searchType(searchType)
    //=========================================================================

    tailRecurse(tcpClient.execute {
                  sr
                }.await(EsClientMaps.awaitDur),
                consumer,
                scrollTimeout,
                if(quitAfter > 0)
                  Some(new AtomicLong(quitAfter))
                else None)
  }

  @tailrec
  private def tailRecurse(searchResponse: RichSearchResponse,
                          consumer: (EsMap) => Unit,
                          scrollTimeout: String,
                          quitAfter: Option[AtomicLong]): Unit = {
    val searchHits = searchResponse.hits
    if(searchHits.length != 0) {
      for(sh <- searchHits
          if quitAfter.isEmpty || (quitAfter.get.getAndDecrement() > 0)) {
        consumer(new EsMap(sh.id, sh.sourceAsMap))
      }
      if(    (searchResponse.scrollId != null)
          && (quitAfter.isEmpty || (quitAfter.get.get > 0)))
        tailRecurse(tcpClient.execute {
                      searchScroll(searchResponse.scrollId, scrollTimeout)
                    }.await(EsClientMaps.awaitDur),
                    consumer,
                    scrollTimeout,
                    quitAfter)
    }
  }
}
