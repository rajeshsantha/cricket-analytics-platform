package com.rajesh.cricket.model.cricsheet

/**
 * Represents a single ball delivery from Cricsheet data.
 *
 * @param matchId           Unique match identifier
 * @param inning            Inning number (1 or 2)
 * @param over              Over number (0-indexed)
 * @param ball              Ball number within the over (1-indexed)
 * @param batsman           Batsman facing the delivery
 * @param bowler            Bowler delivering the ball
 * @param nonStriker        Non-striker batsman
 * @param runsBatsman       Runs scored by the batsman off this ball
 * @param runsExtras        Extra runs (wides, no-balls, byes, leg-byes)
 * @param runsTotal         Total runs from this delivery
 * @param wicketKind        Type of dismissal (None if not out)
 * @param wicketPlayerOut   Name of dismissed player (None if not out)
 */
case class Delivery(
  matchId: String,
  inning: Int,
  over: Int,
  ball: Int,
  batsman: String,
  bowler: String,
  nonStriker: String,
  runsBatsman: Int,
  runsExtras: Int,
  runsTotal: Int,
  wicketKind: Option[String],
  wicketPlayerOut: Option[String]
)
