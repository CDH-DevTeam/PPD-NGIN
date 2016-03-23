package controllers

import play.api._
import play.api.mvc._
import play.api.libs.json._
import play.api.libs.ws._
import play.api.Play.current
import play.api.libs.concurrent.Execution.Implicits._

import scala.concurrent.{Await, Future}
import scala.util.matching.Regex
import scala.util.Random

import java.text.SimpleDateFormat
import java.util.Calendar

import org.joda.time.DateTime
import org.joda.time.format._

case class DateFilter(key: String, startDate: Long, endDate: Long) {}
case class TermFilter(key: String, terms: List[String]) {}
case class SearchQuery(queryType: String, query: String, idSeq: String) {}
case class QueryBuilder(searchQuery: SearchQuery, termFilters: List[TermFilter]) {}

class Application extends Controller {

	/*
	 *	Variables
	 */

	val random = new Random()

	val ES_HOST = Play.current.configuration.getString("es.host").get
	val ES_INDEX = Play.current.configuration.getString("es.index").get
	val ES_USER = Play.current.configuration.getString("es.username").get
	val ES_PW = Play.current.configuration.getString("es.password").get

	val ES_EXEC_HINT_PARAM = "global_ordinals_hash"
	val MAX_SHINGLE_SIZE: Int = 4

	val QUERY_TYPE_TERM: String = "term"
	val QUERY_TYPE_PHRASE: String = "phrase"
	val QUERY_TYPE_WILDCARD: String = "wildcard"

	val QUERY_WILDCARD_SUB: String = "*"
	val QUERY_WILDCARD_ES: String = ".*"

	val MOTIONER_FIRST_DATE_MILLISECONDS: Long = 0L
	val MOTIONER_HIT_RETURN_COUNT: Int = Play.current.configuration.getString("api.hit_return_count").get.toInt
	val MOTIONER_AGG_MAX_COUNT: Int = Play.current.configuration.getString("api.agg_max_count").get.toInt
	val MOTIONER_SCROLL_ALIVE_TIME: String = Play.current.configuration.getString("api.scroll_alive_time").get

	// Mappings decoder, if first value in tuple is defined, the field is of nested type.
	val MOTIONER_MAPPINGS_DECODER: Map[String, Tuple2[String, String]] = Map(
		"parti" -> ("dokintressent", "dokintressent.intressent.partibet"),
		"författare" -> ("dokintressent", "dokintressent.intressent.namn"),
		"titel" -> (null, "dokument.titel"),
		"år" -> (null, "dokument.datum")
	)
	
	/*
	 *	Actions
	 */

	// Index page
	def index = Action {
		Ok(views.html.index("Your new application is ready."))
	}

	// Get information from ES index about types and counts.
	def getMotionerTimelineTotal() = Action.async {

		val ES_TYPE = Play.current.configuration.getString("es.type.motioner").get

		// Create query data object.
		var currentDate = new DateTime()
		var queryData = Json.obj(
			"size" -> 0,
			"aggs" -> Json.obj(
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
		)
			

		WS.url(ES_HOST + ES_INDEX + "/" + ES_TYPE + "/_search")
			.withAuth(ES_USER, ES_PW, WSAuthScheme.BASIC)
			.post(queryData)
			.map { response =>
				if (response.status == 200) {
					Ok(
						Json.obj(
							"buckets" -> (response.json \ "aggregations" \ "date_agg" \ "buckets").as[JsArray],
							"doc_count" -> (response.json \ "hits" \ "total").as[JsNumber],
							"key" -> "total"
						)
					)
				} else {
					InternalServerError(response.body)
				}
			}
			.recover {
				case e: Throwable => BadRequest("Bad request!")
			}
	}

	// Get data and hits for timeline search
	def getMotionerTimelineSearch(searchPhrase: String) = Action.async {

		// Set variables
		val ES_TYPE = Play.current.configuration.getString("es.type.motioner").get

		// Parse search string

		//println(" ")
		//println("Search phrase: " + searchPhrase)

		val queryBuilderList = queryParser(searchPhrase)
		
		/*
		for (qbl <- queryBuilderList) {
			println(qbl.searchQuery.queryType + " : " + qbl.searchQuery.query + " (" + qbl.searchQuery.idSeq + ")")
			for (tf <- qbl.termFilters) {
				print("   " + tf.key + " : ")
				for (t <- tf.terms) {
					print(t + ", ")
				}
				println("")
			}
		}
		*/

		// Create ES query
		var queryData = createTimelineQuery(queryBuilderList)
		
		// Query ES
		WS.url(ES_HOST + ES_INDEX + "/" + ES_TYPE + "/_search")
			.withAuth(ES_USER, ES_PW, WSAuthScheme.BASIC)
			.post(queryData)
			.map { response =>
				if (response.status == 200) {
					Ok(createTimelineResponse(response.json, queryBuilderList))
					//Ok(response.json)
				} else {
					InternalServerError(response.body)
				}
			}
			.recover {
				case e: Throwable => BadRequest("Bad request!")
			}
	}
	
	


	// Get information from ES index about types and counts.
	def getMotionerHitScroll(scrollId: String) = Action.async {

		val ES_TYPE = Play.current.configuration.getString("es.type.motioner").get

		var queryData = Json.obj(
			"scroll" -> MOTIONER_SCROLL_ALIVE_TIME,
			"scroll_id" -> scrollId
		)

		WS.url(ES_HOST + "_search/scroll")
			.withAuth(ES_USER, ES_PW, WSAuthScheme.BASIC)
			.post(queryData)
			.map { response =>
				if (response.status == 200) {
					Ok(response.json)
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

	// Create response
	def createTimelineResponse(responseData: JsValue, queryBuilderList: List[QueryBuilder]): JsValue = {

		// Check if shard failed
		val failedCount: Int = (responseData \ "_shards" \ "failed").as[Int]
		if (failedCount > 0) {
			return Json.obj("message" -> "Elasticsearch query failed.")
		}

		// Include aggregations
		var aggList: List[JsObject] = List()
		var aggObject: List[JsObject] = List()
		for (qbl <- queryBuilderList) {

			// Check if aggregation contains filters
			if (qbl.termFilters.isEmpty) {
				aggObject = (responseData \ "aggregations" \ qbl.searchQuery.idSeq \ "buckets").as[List[JsObject]]
			} else {
				aggObject = (responseData \ "aggregations" \ qbl.searchQuery.idSeq \ "term_agg" \ "buckets").as[List[JsObject]]
			}
			
			// Create list of aggregations.
			for (bucketObject <- aggObject) {
				aggList ::= Json.obj(
					"key" -> (bucketObject \ "key").as[JsString],
					"search_query" -> qbl.searchQuery.query,
					"type" -> qbl.searchQuery.queryType,
					"filters" -> qbl.termFilters.map(tf => Json.obj(tf.key -> tf.terms)),
					"doc_count" -> (bucketObject \ "doc_count").as[JsNumber],
					"buckets" -> (bucketObject \ "date_agg" \ "buckets").as[JsArray]
				)
			}
		}

		aggList = aggList.sortWith {
			(a,b) =>
				(a \ "doc_count").as[Int] > (b \ "doc_count").as[Int]
		}

		return Json.obj(
			"es_query_time" -> (responseData \ "took").as[JsNumber],
			"data" -> Json.toJson(aggList)
		)
	}

	// Create ES queries
	def createTimelineQuery(queryBuilderList: List[QueryBuilder]): JsObject = {

		var queryFilter: List[JsObject] = List()
		var termAgg: List[String] = List()
		var phraseAgg: List[String] = List()
		var wildcardAgg: String = ""

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

		// Create aggregation queries.
		var aggregations: Map[String, JsObject] = Map()
		for (qbl <- queryBuilderList) {

			// Check if shingle field is required.
			var queryField: String = "dokument.html.shingles"
			if (qbl.searchQuery.queryType == QUERY_TYPE_TERM) {
				queryField = "dokument.html"
			}

			// Check if there is any filter for this search.
			if (qbl.termFilters.isEmpty) {
				aggregations += qbl.searchQuery.idSeq -> Json.obj(
					"terms" -> Json.obj(
						"min_doc_count" -> 0,
						"field" -> queryField,
						"execution_hint" -> ES_EXEC_HINT_PARAM,
						"include" -> qbl.searchQuery.query,
						"size" -> MOTIONER_AGG_MAX_COUNT
					),
					"aggs" -> yearAggregation
				)
			} else {
				var filters: List[JsObject] = List()
				for (tf <- qbl.termFilters) {
					if (MOTIONER_MAPPINGS_DECODER.exists(_._1 == tf.key)) {

						// Create filter object.
						var tmpFilter: JsObject = Json.obj(
							"bool" -> Json.obj(
								"should" -> Json.toJson(tf.terms.map(term => Json.obj("match" -> Json.obj(MOTIONER_MAPPINGS_DECODER(tf.key)._2 -> term))))
							)
						)

						// Check if filter field is nested.
						if (MOTIONER_MAPPINGS_DECODER(tf.key)._1 != null) {
							filters ::= Json.obj(
								"nested" -> Json.obj(
									"path" -> MOTIONER_MAPPINGS_DECODER(tf.key)._1,
									"query" -> tmpFilter
								)
							)
						} else {
							filters ::= Json.obj(
								"query" -> tmpFilter
							)
						}
					}
				}

				// Define aggregation object.
				aggregations += qbl.searchQuery.idSeq -> Json.obj(
					"filter" -> Json.obj(
						"bool" -> Json.obj(
							"must" -> Json.toJson(filters)
						)
					),
					"aggs" -> Json.obj(
						"term_agg" -> Json.obj(
							"terms" -> Json.obj(
								"min_doc_count" -> 0,
								"field" -> queryField,
								"execution_hint" -> ES_EXEC_HINT_PARAM,
								"include" -> qbl.searchQuery.query,
								"size" -> MOTIONER_AGG_MAX_COUNT
							),
							"aggs" -> yearAggregation
						)
					)
				)
			}
		}

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
			"aggs" -> Json.toJson(aggregations)
		)

		//println(Json.prettyPrint(queryData))

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
		var inParentheses: Boolean = false
		for (c <- searchPhrase) {

			if (c == '(') inParentheses = true
			else if (c == ')') inParentheses = false

			if (currIndex == (searchPhrase.length() - 1)) {
				splitResult ::= searchPhrase.substring(startIndex).trim()
			} else if (c == ',' && !inParentheses) {
				splitResult ::= searchPhrase.substring(startIndex, currIndex).trim()
				startIndex = currIndex + 1
			}

			currIndex += 1

		}

		// Iterate future buckets
		for (sr <- splitResult) {

			var searchQueries: List[SearchQuery] = List()
			var termFilters: List[TermFilter] = List()

			// Parse splitted term
			var pattern = "(\\S*?):\\(.*?\\)".r
			val filterExtractions = pattern.findAllIn(sr)
			val searchQuery = pattern.replaceAllIn(sr, "")

			// Parse search query
			var wordCount: Int = searchQuery.split(" ").length
			if (searchQuery.contains('(') && searchQuery.contains(')')) { // Multiple terms
				for (sq <- searchQuery.replace('(', ' ').replace(')', ' ').split(",").toList) {
					searchQueries ::= SearchQuery(QUERY_TYPE_TERM, sq.trim(), randomAlphanumericString(10))
				}
			} else {
				if (searchQuery.contains(QUERY_WILDCARD_SUB)) { // Wildcard

					if (wordCount > MAX_SHINGLE_SIZE) {

						var wildCardIndex: Int = searchQuery.split(" ").indexWhere(_.contains(QUERY_WILDCARD_SUB))
						var startSlice: Int =  (wildCardIndex - (MAX_SHINGLE_SIZE - 1))
						var stopSlice: Int = wildCardIndex + 1

						if ((wildCardIndex - MAX_SHINGLE_SIZE) < 0) {
							startSlice =  0
							stopSlice = MAX_SHINGLE_SIZE
						}

						searchQueries ::= SearchQuery(QUERY_TYPE_WILDCARD, searchQuery.split(" ").slice(startSlice, stopSlice).mkString(" ").replace(QUERY_WILDCARD_SUB, QUERY_WILDCARD_ES).trim(), randomAlphanumericString(10))

					} else searchQueries ::= SearchQuery(QUERY_TYPE_WILDCARD, searchQuery.replace(QUERY_WILDCARD_SUB, QUERY_WILDCARD_ES).trim(), randomAlphanumericString(10))

				} else if (wordCount > 1) { // Phrase
					if (wordCount > MAX_SHINGLE_SIZE) searchQueries ::= SearchQuery(QUERY_TYPE_PHRASE, searchQuery.split(" ").take(MAX_SHINGLE_SIZE).mkString(" ").trim(), randomAlphanumericString(10))
					else searchQueries ::= SearchQuery(QUERY_TYPE_PHRASE, searchQuery.trim(), randomAlphanumericString(10))

				} else { // Term
					searchQueries ::= SearchQuery(QUERY_TYPE_TERM, searchQuery.trim(), randomAlphanumericString(10))
				}
			}

			// Parse filters
			for(fe <- filterExtractions) {

				var filterKey: Option[String] = "(.*?)(?=:)".r.findFirstIn(fe.trim)
				var filterParams: Option[String] = "(?<=\\()(.*?)(?=\\))".r.findFirstIn(fe.trim)

				// Check if filter key and params are found
				if (filterKey != None && filterParams != None) {

					/* DATE FILTERS FOR THE FUTURE
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
					}*/

					termFilters ::= TermFilter(filterKey.get, filterParams.get.split(",").map(_.trim).toList)

				}

			}

			// Insert parsed data into query builder object
			for (sq <- searchQueries) {
				queryBuilderList ::= QueryBuilder(sq, termFilters)
			}

		}
		
		return queryBuilderList

	}

	// Generate a random string of length n from the given alphabet
	def randomString(alphabet: String)(n: Int): String = 
		Stream.continually(random.nextInt(alphabet.size)).map(alphabet).take(n).mkString

	// Generate a random alphabnumeric string of length n
	def randomAlphanumericString(n: Int) = 
		randomString("abcdefghijklmnopqrstuvwxyz0123456789")(n)

}