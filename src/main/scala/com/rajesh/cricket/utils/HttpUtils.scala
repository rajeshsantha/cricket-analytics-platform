package com.rajesh.cricket.utils

import sttp.client3._
import io.circe.Json
import io.circe.parser.parse

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/** HTTP utility helpers using sttp with retry and exponential backoff. */
object HttpUtils {

  private val backend: SttpBackend[Identity, Any] = HttpURLConnectionBackend()

  /**
   * Perform a GET request and return the response body as a String.
   *
   * @param url      Target URL
   * @param headers  Optional HTTP headers (e.g., for API key auth)
   * @return         Response body string or throws on error
   */
  def get(url: String, headers: Map[String, String] = Map.empty): String = {
    val request = basicRequest.get(uri"$url").headers(headers).response(asStringAlways)
    val response = backend.send(request)
    response.body
  }

  /**
   * Perform a GET request with exponential backoff retries.
   *
   * @param url        Target URL
   * @param headers    Optional HTTP headers
   * @param maxRetries Maximum number of retry attempts (default 3)
   * @return           Response body string wrapped in Try
   */
  def getWithRetry(
    url: String,
    headers: Map[String, String] = Map.empty,
    maxRetries: Int = 3
  ): Try[String] = {
    @tailrec
    def attempt(retriesLeft: Int, delayMs: Long): Try[String] = {
      val result = Try(get(url, headers))
      result match {
        case Success(body) => Success(body)
        case Failure(ex) if retriesLeft > 0 =>
          Thread.sleep(delayMs)
          attempt(retriesLeft - 1, delayMs * 2) // exponential backoff
        case Failure(ex) => Failure(ex)
      }
    }
    attempt(maxRetries, 500L)
  }

  /**
   * Perform a GET request and parse the response as circe Json.
   *
   * @param url      Target URL
   * @param headers  Optional HTTP headers
   * @return         Parsed Json or throws on parse error
   */
  def getJson(url: String, headers: Map[String, String] = Map.empty): Json = {
    val body = get(url, headers)
    parse(body) match {
      case Right(json) => json
      case Left(err)   => throw new RuntimeException(s"Failed to parse JSON from $url: $err")
    }
  }
}
