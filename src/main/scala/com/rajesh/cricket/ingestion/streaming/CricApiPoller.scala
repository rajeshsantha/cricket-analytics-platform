package com.rajesh.cricket.ingestion.streaming

import com.rajesh.cricket.config.{AppConfig, CricApiConfig}
import com.rajesh.cricket.utils.HttpUtils
import io.circe.Json
import io.circe.parser.parse
import org.apache.logging.log4j.LogManager

import java.time.Instant
import scala.util.{Failure, Success}

/**
 * Polls the CricAPI REST endpoint every N seconds and publishes
 * ball-by-ball events to a Kafka topic.
 *
 * Usage: call startPolling(matchId) to begin the polling loop.
 */
class CricApiPoller(config: CricApiConfig, producer: KafkaProducer) {

  private val logger = LogManager.getLogger(getClass)
  private val topic  = AppConfig.kafkaTopicLiveBalls

  /**
   * Start the polling loop for the given match.
   * Runs indefinitely until interrupted.
   *
   * @param matchId  The CricAPI match ID to poll
   */
  def startPolling(matchId: String): Unit = {
    logger.info(s"Starting CricAPI poller for matchId=$matchId, interval=${config.pollIntervalSeconds}s")
    while (!Thread.currentThread().isInterrupted) {
      try {
        pollOnce(matchId)
      } catch {
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          logger.info("Poller interrupted, stopping.")
        case e: Exception =>
          logger.error(s"Error during poll: ${e.getMessage}", e)
      }
      Thread.sleep(config.pollIntervalSeconds * 1000L)
    }
    logger.info("CricAPI poller stopped.")
  }

  /**
   * Perform a single poll request and publish events to Kafka.
   *
   * @param matchId  The CricAPI match ID
   */
  def pollOnce(matchId: String): Unit = {
    val url     = s"${config.baseUrl}/cricScore?id=$matchId&apikey=${config.apiKey}"
    val headers = Map("Accept" -> "application/json")

    HttpUtils.getWithRetry(url, headers) match {
      case Success(body) =>
        parse(body) match {
          case Right(json) => extractAndPublishBalls(json, matchId)
          case Left(err)   => logger.warn(s"JSON parse error: $err")
        }
      case Failure(ex) =>
        logger.error(s"HTTP request failed after retries: ${ex.getMessage}")
    }
  }

  /**
   * Parse the CricAPI JSON response and publish each ball event as JSON to Kafka.
   *
   * @param json     Parsed circe Json response
   * @param matchId  Match ID for keying Kafka records
   */
  private def extractAndPublishBalls(json: Json, matchId: String): Unit = {
    val cursor = json.hcursor
    val status = cursor.downField("status").as[String].getOrElse("unknown")

    if (status != "success") {
      logger.warn(s"CricAPI returned non-success status: $status")
      return
    }

    // Extract scorecard / ball-by-ball data from the response
    val scorecard = cursor.downField("data").downField("scorecard").focus
    scorecard match {
      case Some(sc) =>
        sc.asArray.foreach { innings =>
          innings.foreach { inningJson =>
            val inningName = inningJson.hcursor.downField("inning").as[String].getOrElse("unknown")
            val overs      = inningJson.hcursor.downField("overs").as[List[Json]].getOrElse(Nil)
            overs.foreach { overJson =>
              val overNum = overJson.hcursor.downField("over").as[Int].getOrElse(0)
              val deliveries = overJson.hcursor.downField("deliveries").as[List[Json]].getOrElse(Nil)
              deliveries.foreach { delivery =>
                val ballEvent = buildBallEvent(delivery, matchId, inningName, overNum)
                producer.send(topic, matchId, ballEvent)
              }
            }
          }
        }
      case None =>
        logger.debug(s"No scorecard data in response for matchId=$matchId")
    }
  }

  /**
   * Build a JSON string representing a single ball event.
   */
  private def buildBallEvent(
    delivery: Json,
    matchId: String,
    inning: String,
    over: Int
  ): String = {
    val c       = delivery.hcursor
    val batsman = c.downField("batsman").as[String].getOrElse("")
    val bowler  = c.downField("bowler").as[String].getOrElse("")
    val ball    = c.downField("ball").as[Int].getOrElse(0)
    val runs    = c.downField("runs").focus.getOrElse(Json.obj())
    val wicket  = c.downField("wickets").focus
    val eventTime = Instant.now().toString

    s"""{
       |  "matchId": "$matchId",
       |  "inning": "$inning",
       |  "over": $over,
       |  "ball": $ball,
       |  "batsman": "$batsman",
       |  "bowler": "$bowler",
       |  "runs": ${runs.noSpaces},
       |  "wicket": ${wicket.map(_.noSpaces).getOrElse("null")},
       |  "eventTime": "$eventTime"
       |}""".stripMargin
  }
}
