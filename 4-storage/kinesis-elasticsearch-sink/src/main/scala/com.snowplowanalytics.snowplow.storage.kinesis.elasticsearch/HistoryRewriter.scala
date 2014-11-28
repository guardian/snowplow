package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch

import com.sksamuel.elastic4s._
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext
import org.slf4j.LoggerFactory
import scala.util.Success
import scala.util.Failure
import scala.Some

trait Logging {
  val LOG = LoggerFactory.getLogger(this.getClass)
}


object HistoryRewriter extends UpdateDsl with SearchDsl with FilterDsl with IndexDsl with BulkDsl with Logging {
  import ExecutionContext.Implicits._

  val elasticsearch = ElasticsearchSinkApp.configValue.getConfig("elasticsearch")
  val clusterName = elasticsearch.getString("cluster-name")
  val elasticsearchEndpoint = {
    ElasticSearchConnection.discoveredElasticsearchHosts(
      elasticsearch.getString("stack"),
      ElasticsearchSinkApp.configValue.getString("stage")
    ).headOption.getOrElse(elasticsearch.getString("endpoint"))
  }

  import org.elasticsearch.common.settings.ImmutableSettings
  val settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build()
  val client = ElasticClient.remote(settings, (elasticsearchEndpoint, 9300))


  def rewrite(userId: String, timestamp: Long) = {
    bulkRewrite(userId, timestamp)
  }

  private def getEvents(userId: String) = {
    val req = search("events") types "enriched" filter {
        missingFilter("timesincesubscription")
    } query {
      bool {
        must (
          termQuery("unstruct_event_1.from.userid", userId)
        ) not termsQuery("unstruct_event_1.from.subscriptionstatus", "resubscriber", "firsttime_subscriber")
      }
    }
    client.execute(req.build)
  }

  private def bulkRewrite(userId: String, timestamp: Long) {
    getEvents(userId) onComplete {
      case Success(res) => {
        val updates = for (event <- res.getHits.hits.toList) yield {
          Option(event.getSource.get("dvce_tstamp")).flatMap {
            case ts:String => {
              val diff = ts.toLong - timestamp
              Some(update(event.getId).in("events/enriched").doc("timesincesubscription" -> diff))
            }
            case _ => None
          }
        }

        client.execute {
          bulk(updates.flatten:_*)
        } onComplete {
          case Success(res) => {
            val result = res.getItems.toList.map(item => Option(item.getFailureMessage)).flatten
            if (result.length > 0) {
              LOG.error(s"failed to update historical record of user $userId: ${result.mkString("", ", ", "")}")
            }
          }
          case Failure(f) => {
            LOG.error(s"failed to update historical record of user $userId")
            LOG.error(f.getMessage)
          }
        }

      }
      case _ =>
    }
  }
}
