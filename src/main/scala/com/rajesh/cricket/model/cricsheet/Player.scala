package com.rajesh.cricket.model.cricsheet

/**
 * Represents a cricket player extracted from Cricsheet match data.
 *
 * @param playerId    Unique player identifier (derived from name + team)
 * @param playerName  Full player name
 * @param team        Team the player belongs to in this match
 * @param matchId     Match in which this player appeared
 */
case class Player(
  playerId: String,
  playerName: String,
  team: String,
  matchId: String
)
