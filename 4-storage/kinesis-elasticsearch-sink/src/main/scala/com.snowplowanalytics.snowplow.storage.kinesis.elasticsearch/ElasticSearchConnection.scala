package com.snowplowanalytics.snowplow.storage.kinesis.elasticsearch

import com.amazonaws.services.ec2.model.{DescribeInstancesRequest, Filter}

import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.ec2.AmazonEC2AsyncClient

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress

import scala.collection.convert.decorateAsJava._

object ElasticSearchConnection {

  def discoveredElasticsearchHosts(stack: String): List[String] = {
    import scala.collection.JavaConversions._
    val hosts = AWS.EC2.describeInstances(
      new DescribeInstancesRequest().withFilters(
        new Filter("tag:Stage", List("TEST")),
        new Filter("tag:App", List("elasticsearch")),
        new Filter("tag:Stack", List(stack))
      ))
    hosts.getReservations.flatMap(_.getInstances).map(_.getPublicDnsName).toList
  }
}




object AWS {

  lazy val region = Region getRegion Regions.EU_WEST_1
  lazy val EC2 = region.createClient(classOf[AmazonEC2AsyncClient], null, null)

}
