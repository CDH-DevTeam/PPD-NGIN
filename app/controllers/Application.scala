package controllers

import play.api._
import play.api.mvc._
import play.api.libs.json._
import play.api.libs.ws._
import play.api.Play.current
import play.api.libs.concurrent.Execution.Implicits._

import scala.concurrent.{Await, Future}
import scala.util.matching.Regex

import java.text.SimpleDateFormat
import java.util.Calendar

import org.joda.time.DateTime
import org.joda.time.format._

case class DateFilter(key: String, startDate: Long, endDate: Long) {}
case class TermFilter(key: String, terms: List[String]) {}
case class SearchQuery(queryType: String, query: String) {}
case class QueryBuilder(searchQuery: SearchQuery, dateFilters: List[DateFilter], termFilters: List[TermFilter]) {}

class Application extends Controller {

	/*
	 *	Variables
	 */

	val ES_HOST = Play.current.configuration.getString("es.host").get
	val ES_INDEX = Play.current.configuration.getString("es.index").get
	val ES_USER = Play.current.configuration.getString("es.username").get
	val ES_PW = Play.current.configuration.getString("es.password").get

	val MAX_SHINGLE_SIZE: Int = 4

	val QUERY_TYPE_TERM: String = "term"
	val QUERY_TYPE_PHRASE: String = "phrase"
	val QUERY_TYPE_WILDCARD: String = "wildcard"

	val MOTIONER_FIRST_DATE_MILLISECONDS: Long = 0L
	
	/*
	 *	Actions
	 */

	// Index page
	def index = Action {
		Ok(views.html.index("Your new application is ready."))
	}

	// Get information from ES index about types and counts.
	def getDocCount(docType: String) = Action.async {

		WS.url(ES_HOST + ES_INDEX + "/" + docType + "/_count")
			.get()
			.map { response =>
				if (response.status == 200) {
					Ok((response.json \ "count").as[JsNumber])
				} else {
					InternalServerError(response.body)
				}
			}
			.recover {
				case e: Throwable => BadRequest("Bad request!")
			}
	}


	
	def getMotionerTimeline(searchPhrase: String) = Action.async {

		// Set variables
		val ES_TYPE = Play.current.configuration.getString("es.type.motioner").get

		// Parse search string

		println(" ")
		println("Search phrase: " + searchPhrase)

		val queryBuilderList = queryParser(searchPhrase)
		
		for (qbl <- queryBuilderList) {
			println(qbl.searchQuery.queryType + " : " + qbl.searchQuery.query)
			for (tf <- qbl.termFilters) {
				print("   " + tf.key + " : ")
				for (t <- tf.terms) {
					print(t + ", ")
				}
				println("")
			}
			for (tf <- qbl.dateFilters) {
				println("   " + tf.key + " : " + tf.startDate + " - " + tf.endDate)
			}
		}

		
		// Create ES query
		var queryData = createTimelineQuery(queryBuilderList)
		
		// Query ES
		WS.url(ES_HOST + ES_INDEX + "/" + ES_TYPE + "/_search")
			.withAuth(ES_USER, ES_PW, WSAuthScheme.BASIC)
			.post(queryData)
			.map { response =>
				if (response.status == 200) {
					Ok(createTimelineResponse(response.json))
					//Ok(response.json)
				} else {
					InternalServerError(response.body)
				}
			}
			.recover {
				case e: Throwable => BadRequest("Bad request!")
			}
	}
	
	/*
	 *	Help Functions
	 */

	def createTimelineResponse(responseData: JsValue): JsValue = {

		// Check if shard failed
		val failedCount: Int = (responseData \ "_shards" \ "failed").as[Int]
		if (failedCount > 0) {
			return Json.obj("message" -> "Elasticsearch query failed.")
		}

		// Include aggregations
		var termAgg: List[JsObject] = List()
		for (pa <- (responseData \ "aggregations" \ "term_agg" \ "buckets").as[List[JsObject]]) {
			println(pa \ "key")
			termAgg ::= Json.obj(
				"key" -> (pa \ "key").as[JsString],
				"type" -> "term",
				"doc_count" -> (pa \ "doc_count").as[JsNumber],
				"buckets" -> (pa \ "date_agg" \ "buckets").as[JsArray]
			)
		}

		var phraseAgg: List[JsObject] = List()
		for (pa <- (responseData \ "aggregations" \ "phrase_agg" \ "buckets").as[List[JsObject]]) {
			println(pa \ "key")
			phraseAgg ::= Json.obj(
				"key" -> (pa \ "key").as[JsString],
				"type" -> "phrase",
				"doc_count" -> (pa \ "doc_count").as[JsNumber],
				"buckets" -> (pa \ "date_agg" \ "buckets").as[JsArray]
			)
		}
		
		var wildcardAgg: List[JsObject] = List()
		for (pa <- (responseData \ "aggregations" \ "wildcard_agg" \ "buckets").as[List[JsObject]]) {
			println(pa \ "key")
			phraseAgg ::= Json.obj(
				"key" -> (pa \ "key").as[JsString],
				"type" -> "wildcard",
				"doc_count" -> (pa \ "doc_count").as[JsNumber],
				"buckets" -> (pa \ "date_agg" \ "buckets").as[JsArray]
			)
		}
		
		return Json.toJson(termAgg ::: phraseAgg ::: wildcardAgg)
	}

	// Create ES queries
	def createTimelineQuery(queryBuilderList: List[QueryBuilder]): JsObject = {
		
		var queryFilter: List[JsObject] = List()
		var termAgg: List[String] = List()
		var phraseAgg: List[String] = List()
		var wildcardAgg: String = ""

		// Find query types and searches, if wildcard, ignore the rest.
		var wildcardIncluded = false
		for (qbl <- queryBuilderList) {
			if (!wildcardIncluded) {
				if (qbl.searchQuery.queryType == QUERY_TYPE_TERM) {
					queryFilter ::= Json.obj("match" -> Json.obj("dokument.html" -> qbl.searchQuery.query))
				}
				else if (qbl.searchQuery.queryType == QUERY_TYPE_PHRASE) {
					queryFilter ::= Json.obj("match_phrase" -> Json.obj("dokument.html.shingles" -> qbl.searchQuery.query))
				}
				else if (qbl.searchQuery.queryType == QUERY_TYPE_WILDCARD) {
					queryFilter = Json.obj("match_phrase" -> Json.obj("dokument.html.shingles" -> qbl.searchQuery.query)) :: Nil
					wildcardIncluded = true
				}
			}
		}

		// Create query aggregations.
		if (!wildcardIncluded) {
			for (qbl <- queryBuilderList) {
				if (qbl.searchQuery.queryType == QUERY_TYPE_TERM) {
					termAgg ::= qbl.searchQuery.query
				}
				else if (qbl.searchQuery.queryType == QUERY_TYPE_PHRASE) {
					phraseAgg ::= qbl.searchQuery.query
				}
			}
		} else {
			for (qbl <- queryBuilderList) {
				if (qbl.searchQuery.queryType == QUERY_TYPE_WILDCARD) {
					wildcardAgg = qbl.searchQuery.query
				}
			}
		}
		
		// Create query data object.
		var currentDate = new DateTime()
		var yearAggregation: JsObject = Json.obj(
			"date_agg" -> Json.obj(
				"date_histogram" -> Json.obj(
					"field" -> "dokument.datum",
					"interval" -> "year",
					"format" -> "yyyy",
					"min_doc_count" -> 0,
					"extended_bounds" -> Json.obj(
						"min" -> MOTIONER_FIRST_DATE_MILLISECONDS,
						"max" -> currentDate.getMillis()
					)
				)
			)
		)
		var queryData = Json.obj(
			"size" -> 0,
			"query" -> Json.obj(
				"filtered" -> Json.obj(
					"filter" -> Json.obj(
						"bool" -> Json.obj(
							"should" -> Json.toJson(queryFilter)
						)
					)
				)
			),
			"aggs" -> Json.obj(
				"term_agg" -> Json.obj(
					"terms" -> Json.obj(
						"field" -> "dokument.html",
						"include" -> Json.toJson(termAgg)
					),
					"aggs" -> yearAggregation
				),
				"phrase_agg" -> Json.obj(
					"terms" -> Json.obj(
						"field" -> "dokument.html.shingles",
						"include" -> Json.toJson(phraseAgg)
					),
					"aggs" -> yearAggregation
				),
				"wildcard_agg" -> Json.obj(
					"terms" -> Json.obj(
						"field" -> "dokument.html.shingles",
						"include" -> wildcardAgg,
						"size" -> 20
					),
					"aggs" -> yearAggregation
				)
			)
		)

		return queryData
	}

	// Parser for search queries
	def queryParser(searchPhrase: String) : List[QueryBuilder] = {

		// Declare return object
		var queryBuilderList: List[QueryBuilder] = List()

		// Split search terms
		var splitResult: List[String] = List()
		var startIndex: Int = 0
		var currIndex: Int = 0
		var inQuotes: Boolean = false
		var inParentheses: Boolean = false
		for (c <- searchPhrase) {

			if (c == '"') inQuotes = !inQuotes

			if (c == '(') inParentheses = true
			else if (c == ')') inParentheses = false

			if (currIndex == (searchPhrase.length() - 1)) {
				splitResult ::= searchPhrase.substring(startIndex).trim()
			} else if (c == ',' && !inQuotes && !inParentheses) {
				splitResult ::= searchPhrase.substring(startIndex, currIndex).trim()
				startIndex = currIndex + 1
			}

			currIndex += 1

		}

		// Iterate future buckets
		for (sr <- splitResult) {

			var searchQueries: List[SearchQuery] = List()
			var dateFilters: List[DateFilter] = List()
			var termFilters: List[TermFilter] = List()


			// Parse splitted term
			var pattern = "(\\S*?):\\(.*?\\)".r
			val filterExtractions = pattern.findAllIn(sr)
			val searchQuery = pattern.replaceAllIn(sr, "")

			// Parse search query
			var wordCount: Int = searchQuery.split(" ").length
			if (searchQuery.contains('"')) { // Multiple terms
				for (sq <- searchQuery.replace('"', ' ').split(",").toList) {
					searchQueries ::= SearchQuery(QUERY_TYPE_TERM, sq.trim())
				}
			} else {
				if (searchQuery.contains('*')) { // Wildcard

					if (wordCount > MAX_SHINGLE_SIZE) {

						var wildCardIndex: Int = searchQuery.split(" ").indexWhere(_.contains("*"))
						var startSlice: Int =  (wildCardIndex - (MAX_SHINGLE_SIZE - 1))
						var stopSlice: Int = wildCardIndex + 1

						if ((wildCardIndex - MAX_SHINGLE_SIZE) < 0) {
							startSlice =  0
							stopSlice = MAX_SHINGLE_SIZE
						}

						searchQueries ::= SearchQuery(QUERY_TYPE_WILDCARD, searchQuery.split(" ").slice(startSlice, stopSlice).mkString(" ").trim())

					} else searchQueries ::= SearchQuery(QUERY_TYPE_WILDCARD, searchQuery.trim())

				} else if (wordCount > 1) { // Phrase
					if (wordCount > MAX_SHINGLE_SIZE) searchQueries ::= SearchQuery(QUERY_TYPE_PHRASE, searchQuery.split(" ").take(MAX_SHINGLE_SIZE).mkString(" ").trim())
					else searchQueries ::= SearchQuery(QUERY_TYPE_PHRASE, searchQuery.trim())

				} else { // Term
					searchQueries ::= SearchQuery(QUERY_TYPE_TERM, searchQuery.trim())
				}
			}

			// Parse filters
			for(fe <- filterExtractions) {

				var filterKey: Option[String] = "(.*?)(?=:)".r.findFirstIn(fe.trim)
				var filterParams: Option[String] = "(?<=\\()(.*?)(?=\\))".r.findFirstIn(fe.trim)

				// Check if filter key and params are found
				if (filterKey != None && filterParams != None) {

					// Check if filter params contain year segement or term filters
					if (filterParams.get.contains("-")) {
						var startDate: String = "(\\d*?)(?=\\-)".r.findFirstIn(filterParams.get).get
						var endDate: String = "(?<=\\-)(\\d*?)($|\\,|\\s)".r.findFirstIn(filterParams.get).get

						// Parse dates
						var dateParsers: Array[DateTimeParser] = Array(
							DateTimeFormat.forPattern("yyyyMMdd").getParser(),
							DateTimeFormat.forPattern("yy").getParser(),
							DateTimeFormat.forPattern("yyyy").getParser()
						)
						var dateFormatter: DateTimeFormatter = new DateTimeFormatterBuilder().append(null, dateParsers).toFormatter();
						var startDateObject: DateTime = dateFormatter.parseDateTime(startDate)
						var endDateObject: DateTime = dateFormatter.parseDateTime(endDate)

						dateFilters ::= DateFilter(filterKey.get, startDateObject.getMillis(), endDateObject.getMillis())


					} else {
						termFilters ::= TermFilter(filterKey.get, filterParams.get.split(",").map(_.trim).toList)
					}

				}

			}

			// Insert parsed data into query builder object
			for (sq <- searchQueries) {
				queryBuilderList ::= QueryBuilder(sq, dateFilters, termFilters)
			}

		}
		
		return queryBuilderList

	}

}





/*

def test = Action.async {

	Future {
		Thread.sleep(2000)
		"tjenare mannen"
	}.map {
		res => Ok(res)
	}

	/*
	val ok = Ok("Hello world!")
	val notFound = NotFound
	val pageNotFound = NotFound(<h1>Page not found</h1>)
	val badRequest = BadRequest(views.html.form(formWithErrors))
	val oops = InternalServerError("Oops")
	val anyStatus = Status(488)("Strange response type")
	*/

}


def get_motioner_all() = Action {
	print("get motioner all")
	Ok("Get all motioner")
}

def testRequest = Action.async {

	val data = Json.obj(
		"size" -> "0",
		"query" -> Json.obj(
			"match_phrase" -> Json.obj(
				"anforandetext.anforandetext_shingles" -> "mer pengar till"
			)
		),
		"aggs" -> Json.obj(
			"ngram_agg" -> Json.obj(
				"terms" -> Json.obj(
					"field" -> "anforandetext.anforandetext_shingles",
					"include" -> "mer pengar till .*",
					"size" -> 10
				)
			)
		)
	)

	WS.url(ES_HOST + "ppd/type_anforande/_seach?pretty=true")
		.post(data)
		.map { response =>
			Ok(response.body)
		}

}

*/