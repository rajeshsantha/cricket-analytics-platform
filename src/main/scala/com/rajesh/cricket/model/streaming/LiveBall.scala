package com.rajesh.cricket.model.streaming

import java.sql.Timestamp

/**
 * Represents a single live ball delivery from the CricAPI stream.
 *
 * @param matchId       Unique match identifier
 * @param inning        Inning description (e.g., "1st Innings")
 * @param over          Over number (0-indexed)
 * @param ball          Ball number within the over
 * @param batsman       Batsman facing the delivery
 * @param bowler        Bowler delivering the ball
 * @param runsBatsman   Runs scored by batsman off this ball
 * @param runsExtras    Extra runs conceded
 * @param runsTotal     Total runs from this delivery
 * @param wicketKind    Type of dismissal (None if not out)
 * @param isWicket      Whether this ball resulted in a wicket
 * @param eventTime     Timestamp of the ball event
 */
case class LiveBall(
  matchId: String,
  inning: String,
  over: Int,
  ball: Int,
  batsman: String,
  bowler: String,
  runsBatsman: Int,
  runsExtras: Int,
  runsTotal: Int,
  wicketKind: Option[String],
  isWicket: Boolean,
  eventTime: Timestamp
)
