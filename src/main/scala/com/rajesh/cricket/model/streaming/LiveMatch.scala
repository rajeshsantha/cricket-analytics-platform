package com.rajesh.cricket.model.streaming

import java.sql.Timestamp

/**
 * Represents a live match summary from the CricAPI stream.
 *
 * @param matchId    Unique match identifier
 * @param team1      First team name
 * @param team2      Second team name
 * @param venue      Venue/stadium name
 * @param matchType  Match format: "T20", "ODI", "Test"
 * @param status     Current match status (e.g., "live", "completed")
 * @param eventTime  Timestamp when this record was captured
 */
case class LiveMatch(
  matchId: String,
  team1: String,
  team2: String,
  venue: String,
  matchType: String,
  status: String,
  eventTime: Timestamp
)
