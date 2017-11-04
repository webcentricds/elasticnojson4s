package com.charter.sia.es

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.TcpClient
import com.typesafe.scalalogging.LazyLogging
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse

class EsIndex(val tcpClient: TcpClient) extends LazyLogging {
  def exists(indexName: String): Boolean =
    tcpClient.execute {
      index exists indexName
    }.await.isExists

  def create(indexName: String): CreateIndexResponse =
    tcpClient.execute {
      logger.info("Creating Index: " + indexName)
      createIndex(indexName)
    }.await

  def delete(indexName: String): DeleteIndexResponse =
    tcpClient.execute {
      logger.info("Deleting Index: " + indexName)
      deleteIndex(indexName)
    }.await

  def deleteIfExists(indexName: String): Unit = {
    if (exists(indexName)) {
      logger.info("Deleting Existing Index: " + indexName)
      delete(indexName)
    } else {
      logger.info("Deleting Existing Index " + indexName + " does not exists.  Doing Nothing.")
    }
  }
}
