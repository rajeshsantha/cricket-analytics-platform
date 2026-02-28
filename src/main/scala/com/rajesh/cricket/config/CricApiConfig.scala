package com.rajesh.cricket.config

/**
 * Typed configuration for the CricAPI integration.
 *
 * @param baseUrl             Base URL for the CricAPI REST API
 * @param apiKey              API key (sourced from env var CRICAPI_KEY)
 * @param pollIntervalSeconds How often (in seconds) to poll for new ball data
 * @param matchId             Match ID to stream (sourced from env var CRICAPI_MATCH_ID)
 */
case class CricApiConfig(
  baseUrl: String,
  apiKey: String,
  pollIntervalSeconds: Int,
  matchId: String
)
