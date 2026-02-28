package com.rajesh.cricket.model.streaming

/**
 * Wraps the raw CricAPI JSON response envelope.
 *
 * @param apiId   Unique response identifier returned by CricAPI
 * @param status  Response status ("success" or "failure")
 * @param data    Raw JSON string of the data payload
 */
case class CricApiResponse(
  apiId: String,
  status: String,
  data: String
)
