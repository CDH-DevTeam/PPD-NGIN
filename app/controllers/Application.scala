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
	val MOTIONER_HIT_RETURN_COUNT: Int = 4
	val MOTIONER_SHINGLE_COUNT: Int = 20
	val MOTIONER_SCROLL_ALIVE_TIME: String = "1m"

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

	def createTimelineResponse(responseData: JsValue): JsValue = {

		// Check if shard failed
		val failedCount: Int = (responseData \ "_shards" \ "failed").as[Int]
		if (failedCount > 0) {
			return Json.obj("message" -> "Elasticsearch query failed.")
		}

		// Include aggregations
		var termAgg: List[JsObject] = List()
		for (pa <- (responseData \ "aggregations" \ "term_agg" \ "buckets").as[List[JsObject]].reverse) {
			termAgg ::= Json.obj(
				"key" -> (pa \ "key").as[JsString],
				"type" -> "term",
				"doc_count" -> (pa \ "doc_count").as[JsNumber],
				"buckets" -> (pa \ "date_agg" \ "buckets").as[JsArray]
			)
		}

		var phraseAgg: List[JsObject] = List()
		for (pa <- (responseData \ "aggregations" \ "phrase_agg" \ "buckets").as[List[JsObject]].reverse) {
			phraseAgg ::= Json.obj(
				"key" -> (pa \ "key").as[JsString],
				"type" -> "phrase",
				"doc_count" -> (pa \ "doc_count").as[JsNumber],
				"buckets" -> (pa \ "date_agg" \ "buckets").as[JsArray]
			)
		}
		
		var wildcardAgg: List[JsObject] = List()
		for (pa <- (responseData \ "aggregations" \ "wildcard_agg" \ "buckets").as[List[JsObject]].reverse) {
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
		//var mustFilters: List[JsObject] = List()

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

		// Create query filters
		/*
		for (qbl <- queryBuilderList) {
			for (tf <- qbl.termFilters) {
				if (MOTIONER_MAPPINGS_DECODER(tf.key)._1 != null) {
					mustFilters ::= Json.obj(
						"nested" -> Json.obj(
							"path" -> MOTIONER_MAPPINGS_DECODER(tf.key)._1,
							"query" -> Json.obj(
								"bool" -> Json.obj(
									"must" -> Json.toJson(tf.terms.map(term => Json.obj("match" -> Json.obj(MOTIONER_MAPPINGS_DECODER(tf.key)._2 -> term))))
								)
							)
						)
					)
				} else {
					for (term <- tf.terms) {
						mustFilters ::= Json.obj(
							"match" -> Json.obj(
								MOTIONER_MAPPINGS_DECODER(tf.key)._2 -> term
							)
						)
					}
				}
			}

			for (tf <- qbl.dateFilters) {
				mustFilters ::= Json.obj(
					"range" -> Json.obj(
						MOTIONER_MAPPINGS_DECODER(tf.key)._2 -> Json.obj(
							"gte" -> tf.startDate,
							"lte" -> tf.endDate,
							"format" -> "epoch_millis"
						)
					)
				)

			}
		}
		*/
		
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

		// "must" -> Json.toJson(mustFilters)
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
						"size" -> JsNumber(MOTIONER_SHINGLE_COUNT)
					),
					"aggs" -> yearAggregation
				)
			)
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