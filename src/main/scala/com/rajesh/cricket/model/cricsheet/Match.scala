package com.rajesh.cricket.model.cricsheet

/**
 * Represents a cricket match from Cricsheet data.
 *
 * @param matchId       Unique match identifier
 * @param team1         First team name
 * @param team2         Second team name
 * @param venue         Venue/stadium name
 * @param date          Match date (ISO 8601 string)
 * @param matchType     Match format: "T20", "ODI", "Test"
 * @param winner        Winning team name (None if no result)
 * @param tossWinner    Team that won the toss
 * @param tossDecision  Toss decision: "bat" or "field"
 */
case class Match(
  matchId: String,
  team1: String,
  team2: String,
  venue: String,
  date: String,
  matchType: String,
  winner: Option[String],
  tossWinner: String,
  tossDecision: String
)
