 /*
  * Copyright (c) 2013-2014 Snowplow Analytics Ltd.
  * All rights reserved.
  *
  * This program is licensed to you under the Apache License Version 2.0,
  * and you may not use this file except in compliance with the Apache
  * License Version 2.0.
  * You may obtain a copy of the Apache License Version 2.0 at
  * http://www.apache.org/licenses/LICENSE-2.0.
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the Apache License Version 2.0 is distributed
  * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
  * either express or implied.
  *
  * See the Apache License Version 2.0 for the specific language
  * governing permissions and limitations there under.
  */

package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch

// Amazon
import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration
import com.amazonaws.services.kinesis.connectors.elasticsearch.{
  ElasticsearchEmitter,
  ElasticsearchObject
}
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter

// Elasticsearch
import org.elasticsearch.action.admin.cluster.health.{
  ClusterHealthRequestBuilder,
  ClusterHealthResponse,
  ClusterHealthStatus
}
import org.elasticsearch.action.bulk.{
  BulkItemResponse,
  BulkRequestBuilder,
  BulkResponse
}
import org.elasticsearch.action.bulk.BulkItemResponse.Failure
import org.elasticsearch.action.index.IndexRequestBuilder
import org.elasticsearch.client.transport.{
  NoNodeAvailableException,
  TransportClient
}
import org.elasticsearch.common.settings.{
  ImmutableSettings,
  Settings
}
import org.elasticsearch.common.transport.InetSocketTransportAddress

import com.amazonaws.services.kinesis.connectors.{
  KinesisConnectorConfiguration,
  UnmodifiableBuffer
}

// Java
import java.io.IOException
import java.util.ArrayList
import java.util.List

// Scala
import scala.collection.JavaConversions._

// Scalaz
import scalaz._
import Scalaz._

// Logging
import org.apache.commons.logging.{
  Log,
  LogFactory
}

// TODO use a package object
import SnowplowRecord._

// TODO make this file more functional

/**
 * Class to send valid records to Elasticsearch and invalid records to Kinesis
 */
class SnowplowElasticsearchEmitter(configuration: KinesisConnectorConfiguration)
  extends IEmitter[EmitterInput] {

  private val Log = LogFactory.getLog(getClass)

  /**
   * The settings key for the cluster name.
   * 
   * Defaults to elasticsearch.
   */
  private val ElasticsearchClusterNameKey = "cluster.name"

  /**
   * The settings key for transport client sniffing. If set to true, this instructs the TransportClient to
   * find all nodes in the cluster, providing robustness if the original node were to become unavailable.
   * 
   * Defaults to false.
   */
  private val ElasticsearchClientTransportSniffKey = "client.transport.sniff"

  /**
   * The settings key for ignoring the cluster name. Set to true to ignore cluster name validation
   * of connected nodes.
   * 
   * Defaults to false.
   */
  private val ElasticsearchClientTransportIgnoreClusterNameKey = "client.transport.ignore_cluster_name"

  /**
   * The settings key for ping timeout. The time to wait for a ping response from a node.
   * 
   * Default to 5s.
   */
  private val ElasticsearchClientTransportPingTimeoutKey = "client.transport.ping_timeout"

  /**
   * The settings key for node sampler interval. How often to sample / ping the nodes listed and connected.
   * 
   * Defaults to 5s
   */
  private val ElasticsearchClientTransportNodesSamplerIntervalKey = "client.transport.nodes_sampler_interval"

  private val settings = ImmutableSettings.settingsBuilder
    .put(ElasticsearchClusterNameKey,                         configuration.ELASTICSEARCH_CLUSTER_NAME)
    .put(ElasticsearchClientTransportSniffKey,                configuration.ELASTICSEARCH_TRANSPORT_SNIFF)
    .put(ElasticsearchClientTransportIgnoreClusterNameKey,    configuration.ELASTICSEARCH_IGNORE_CLUSTER_NAME)
    .put(ElasticsearchClientTransportPingTimeoutKey,          configuration.ELASTICSEARCH_PING_TIMEOUT)
    .put(ElasticsearchClientTransportNodesSamplerIntervalKey, configuration.ELASTICSEARCH_NODE_SAMPLER_INTERVAL)
    .build

  /**
   * The Elasticsearch client.
   */
  private val elasticsearchClient = new TransportClient(settings)

  /**
   * The Elasticsearch endpoint.
   */
  private val elasticsearchEndpoint = configuration.ELASTICSEARCH_ENDPOINT

  /**
   * The Elasticsearch port.
   */
  private val elasticsearchPort = configuration.ELASTICSEARCH_PORT

  /**
   * The amount of time to wait in between unsuccessful index requests (in milliseconds).
   * 10 seconds = 10 * 1000 = 10000
   */
  private val BACKOFF_PERIOD = 10000

  elasticsearchClient.addTransportAddress(new InetSocketTransportAddress(elasticsearchEndpoint, elasticsearchPort))
       
  Log.info("ElasticsearchEmitter using elasticsearch endpoint " + elasticsearchEndpoint + ":" + elasticsearchPort)

  /**
   * Emits good records to Elasticsearch and bad records to Kinesis. 
   * All valid records in the buffer get sent to Elasticsearch in a bulk request.
   * All invalid requests and all requests which failed transformation get sent to Kinesis.
   *
   * @param buffer BasicMemoryBuffer containing EmitterInputs
   * @return list of inputs which failed transformation or which Elasticsearch rejected
   */
  @throws[IOException]
  override def emit(buffer: UnmodifiableBuffer[EmitterInput]): List[EmitterInput] = {
    val records = buffer.getRecords
    if (records.isEmpty) {
        return Nil
    }

    val bulkRequest: BulkRequestBuilder = elasticsearchClient.prepareBulk()
    val successfulRecords = records.filter(_._2.isSuccess)
    val unsuccessfulRecords = new ArrayList[EmitterInput]

    for {
      recordTuple <- records
    } yield {
      recordTuple._2.fold(
        badRecord => unsuccessfulRecords.add(recordTuple._1 -> badRecord.fail),
        validRecord => {
          val indexRequestBuilder =
            elasticsearchClient.prepareIndex(validRecord.getIndex, validRecord.getType, validRecord.getId)
          indexRequestBuilder.setSource(validRecord.getSource)
          val version = validRecord.getVersion
          if (version != null) {
            indexRequestBuilder.setVersion(version)
          }
          val ttl = validRecord.getTtl
          if (ttl != null) {
            indexRequestBuilder.setTTL(ttl)
          }
          val create = validRecord.getCreate
          if (create != null) {
            indexRequestBuilder.setCreate(create)
          }
          bulkRequest.add(indexRequestBuilder)
        }
      )
    }

    if (successfulRecords.length > 0) {
      while (true) {
        try {
            val bulkResponse = bulkRequest.execute.actionGet
            val responses = bulkResponse.getItems
            val failures = new ArrayList[EmitterInput]
            failures.addAll(unsuccessfulRecords)
            var numberOfSkippedRecords = 0
            for (i <- 0 until responses.length) {
              if (responses(i).isFailed) {
                Log.error("Record failed with message: " + responses(i).getFailureMessage)
                val failure = responses(i).getFailure
                if (failure.getMessage.contains("DocumentAlreadyExistsException")
                    || failure.getMessage.contains("VersionConflictEngineException")) {
                  numberOfSkippedRecords += 1
                } else {
                  failures.add(successfulRecords.get(i)._1 -> scala.collection.immutable.List(failure.getMessage).fail)
                }
              }
            }
            Log.info("Emitted " + (records.size - failures.size - numberOfSkippedRecords)
              + " records to Elasticsearch")
            if (!failures.isEmpty) {
              printClusterStatus
              Log.warn("Returning " + failures.size + " records as failed")
            }
            return failures
        } catch {
          case nnae: NoNodeAvailableException => {
            Log.error("No nodes found at " + elasticsearchEndpoint + ":" + elasticsearchPort + ". Retrying in "
              + BACKOFF_PERIOD + " milliseconds", nnae)
            sleep(BACKOFF_PERIOD)
          }
          case e: Exception => {
            Log.error("ElasticsearchEmitter threw an unexpected exception ", e)
            sleep(BACKOFF_PERIOD)
          }
        }
      }

      // The compiler requires this
      throw new IllegalStateException("The while loop should only exit when the emit method returns")

    } else {
      unsuccessfulRecords
    }
  }

  /**
   * Handles records rejected by the SnowplowElasticsearchTransformer or by Elasticsearch
   *
   * @param records List of failed records
   */
  override def fail(records: java.util.List[EmitterInput]) {
    // TODO: send the records to Kinesis
    println(records)
  }

  /**
   * Closes the Elasticsearch client which the KinesisConnectorRecordProcessor is shut down
   */
  override def shutdown {
    elasticsearchClient.close
  }

  /**
   * Period between retrying sending events to Elasticsearch
   *
   * @param sleepTime Length of time between tries
   */
  private def sleep(sleepTime: Long): Unit = {
    try {
      Thread.sleep(sleepTime)
    } catch {
      case e: InterruptedException => ()
    }
  }

  /**
   * Logs the Elasticsearch cluster's health
   */
  private def printClusterStatus {
    val healthRequestBuilder = elasticsearchClient.admin.cluster.prepareHealth()
    val response = healthRequestBuilder.execute.actionGet
    if (response.getStatus.equals(ClusterHealthStatus.RED)) {
      Log.error("Cluster health is RED. Indexing ability will be limited")
    } else if (response.getStatus.equals(ClusterHealthStatus.YELLOW)) {
      Log.warn("Cluster health is YELLOW.")
    } else if (response.getStatus.equals(ClusterHealthStatus.GREEN)) {
      Log.info("Cluster health is GREEN.")
    }
  }

}