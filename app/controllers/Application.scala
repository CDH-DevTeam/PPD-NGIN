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
import scala.util.{Success, Failure}
import scala.collection.mutable.ListBuffer

import org.joda.time.DateTime
import org.joda.time.format._

case class DateFilter(key: String, startDate: Long, endDate: Long) {}
case class TermFilter(key: String, terms: List[String]) {}
case class SearchQuery(queryType: String, query: String, idSeq: String) {}
case class QueryBuilder(searchQuery: SearchQuery, termFilters: ListBuffer[TermFilter]) {}
case class DateSpan(startDate: DateTime, endDate: DateTime) {}

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
	val ES_EXEC_SEARCH_PARAM = "breadth_first"
	val MAX_SHINGLE_SIZE: Int = 4

	val QUERY_TYPE_TERM: String = "term"
	val QUERY_TYPE_PHRASE: String = "phrase"
	val QUERY_TYPE_WILDCARD: String = "wildcard"

	val QUERY_WILDCARD_SUB: String = "*"
	val QUERY_WILDCARD_ES: String = ".*"

	val QUERY_MODES: List[String] = List("exact", "spanNear", "spanNearOrdinal", "anywhere")

	val MOTIONER_FIRST_DATE_MILLISECONDS: Long = 0L
	val MOTIONER_HIT_RETURN_COUNT: Int = Play.current.configuration.getString("api.hit_return_count").get.toInt
	val MOTIONER_AGG_MAX_COUNT: Int = Play.current.configuration.getString("api.agg_max_count").get.toInt
	val SPAN_NEAR_SLOP_COUNT: Int = Play.current.configuration.getString("api.span_near_slop_count").get.toInt

	// Mappings decoder, if first value in tuple is defined, the field is of nested type.
	val MOTIONER_MAPPINGS_DECODER: Map[String, Tuple3[String, String, String]] = Map(
		"parti" -> ("dokintressent", "dokintressent.intressent.partibet", "match"),
		"intressent" -> ("dokintressent", "dokintressent.intressent.namn", "match_phrase"),
		"titel" -> (null, "dokument.titel", "match_phrase"),
		"datum" -> (null, "dokument.datum", "range")
	)

	// Check host url
	var HOST_URL_APPEND = if (ES_HOST.takeRight(1) == "/") "" else "/"
	
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
			

		WS.url(ES_HOST + HOST_URL_APPEND + ES_INDEX + "/" + ES_TYPE + "/_search")
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

	// Get data and hits for timeline search.
	def getMotionerTimelineSearch(searchPhrase: String, queryMode: String) = Action.async {

		// Set variables.
		val ES_TYPE = Play.current.configuration.getString("es.type.motioner").get
		val parsedQueryMode = checkQueryMode(queryMode)

		// Parse search params.
		val queryBuilderList = queryParser(searchPhrase)

		// Analyze search queries.
		var futureList: ListBuffer[Future[ListBuffer[String]]] = ListBuffer()
		for (qbl <- queryBuilderList) {
			futureList += fetchAnalyzedQuery(qbl, parsedQueryMode)
		}

		// When analyzed search queries are ready.
		Future.sequence(futureList).flatMap { analyzedQueries => 

			// Create ES query
			var queryData = createTimelineQuery(queryBuilderList, analyzedQueries, parsedQueryMode)

			// Store queries
			storeSearchQueries(queryBuilderList)

			var futureResponse: Future[WSResponse] = WS.url(ES_HOST + HOST_URL_APPEND + ES_INDEX + "/" + ES_TYPE + "/_search")
				.withAuth(ES_USER, ES_PW, WSAuthScheme.BASIC)
				.post(queryData)
			
			futureResponse.map(
				response =>
					if (response.status == 200) {
						Ok(createTimelineResponse(response.json, queryBuilderList, parsedQueryMode))
					} else {
						InternalServerError(response.body)
					}
			)

		}

	}
	
	
	def getMotionerHits(searchPhrase: String, startDate: String, endDate: String, fromIndex: Int, queryMode: String) = Action.async {
		
		val ES_TYPE = Play.current.configuration.getString("es.type.motioner").get
		val parsedQueryMode = checkQueryMode(queryMode)
		
		// Parse search params.
		val queryBuilderList = queryParser(searchPhrase)
		val dateSpan = dateParser(startDate, endDate)

		// Analyze search queries.
		var futureList: ListBuffer[Future[ListBuffer[String]]] = ListBuffer()
		for (qbl <- queryBuilderList) {
			futureList += fetchAnalyzedQuery(qbl, parsedQueryMode)
		}

		// When analyzed search queries are ready.
		Future.sequence(futureList).flatMap { analyzedQueries => 

			// Create query.
			var queryData: Map[String, JsObject] = createHitlistQuery(queryBuilderList, analyzedQueries, dateSpan, fromIndex, parsedQueryMode)
			var queryString: String = ""

			for (qbl <- queryBuilderList) {
				queryString += "{}\n"
				queryString += Json.stringify(queryData(qbl.searchQuery.idSeq)) + "\n"
			}

			var futureResponse: Future[WSResponse] = WS.url(ES_HOST + HOST_URL_APPEND + ES_INDEX + "/" + ES_TYPE + "/_msearch")
				.withAuth(ES_USER, ES_PW, WSAuthScheme.BASIC)
				.post(queryString)

			futureResponse.map(
				response =>
					if (response.status == 200) {
						Ok(createHitlistResponse(response.json, queryBuilderList, dateSpan, fromIndex))
					} else {
						InternalServerError(response.body)
					}
			)

		}

	}

	def getMotionerPartyBarchart(searchPhrase: String, startDate: String, endDate: String, queryMode: String) = Action.async {

		// Set variables.
		val ES_TYPE = Play.current.configuration.getString("es.type.motioner").get
		val parsedQueryMode = checkQueryMode(queryMode)

		// Parse search params.
		val queryBuilderList = queryParser(searchPhrase)
		val dateSpan = dateParser(startDate, endDate)

		// Analyze search queries.
		var futureList: ListBuffer[Future[ListBuffer[String]]] = ListBuffer()
		for (qbl <- queryBuilderList) {
			futureList += fetchAnalyzedQuery(qbl, parsedQueryMode)
		}

		// When analyzed search queries are ready.
		Future.sequence(futureList).flatMap { analyzedQueries => 

			// Create ES query
			var queryData = createBarchartQuery(queryBuilderList, analyzedQueries, dateSpan, parsedQueryMode)

			var futureResponse: Future[WSResponse] = WS.url(ES_HOST + HOST_URL_APPEND + ES_INDEX + "/" + ES_TYPE + "/_search")
				.withAuth(ES_USER, ES_PW, WSAuthScheme.BASIC)
				.post(queryData)

			futureResponse.map(
				response =>
					if (response.status == 200) {
						Ok(createBarchartResponse(response.json, queryBuilderList, dateSpan, parsedQueryMode))
					} else {
						InternalServerError(response.body)
					}
			)

		}

	}

	// Get top search queries.
	def getQueriesTop() = Action.async {

		val ES_INDEX_QUERY = Play.current.configuration.getString("es.queries.index").get
		val ES_TYPE_SEARCH_QUERY = Play.current.configuration.getString("es.queries.type.search_query").get

		// Create query data object.
		var currentDate = new DateTime()
		var queryData = Json.obj(
			"size" -> 0,
			"query" -> Json.obj(
				"match_all" -> Json.obj()
			),
			"aggs" -> Json.obj(
				"top_queries" -> Json.obj(
					"terms" -> Json.obj(
						"field" -> "term",
						"size" -> 10
					)
				)
			)
		)
			
		// Make request to ES.
		WS.url(ES_HOST + HOST_URL_APPEND + ES_INDEX_QUERY + "/" + ES_TYPE_SEARCH_QUERY + "/_search")
			.withAuth(ES_USER, ES_PW, WSAuthScheme.BASIC)
			.post(queryData)
			.map { response =>
				if (response.status == 200) {
					Ok((response.json \ "aggregations" \ "top_queries" \ "buckets").as[JsArray])
				} else {
					InternalServerError(response.body)
				}
			}
			.recover {
				case e: Throwable => BadRequest("Bad request!")
			}

	}

	// Get latest search queries.
	def getQueriesLatest() = Action.async {

		val ES_INDEX_QUERY = Play.current.configuration.getString("es.queries.index").get
		val ES_TYPE_SEARCH_QUERY = Play.current.configuration.getString("es.queries.type.search_query").get

		// Create query data object.
		var currentDate = new DateTime()
		var queryData = Json.obj(
			"size" -> 10,
			"query" -> Json.obj(
				"match_all" -> Json.obj()
			),
			"sort" -> Json.obj("date" -> Json.obj("order" -> "desc"))
		)
			
		// Make request to ES.
		WS.url(ES_HOST + HOST_URL_APPEND + ES_INDEX_QUERY + "/" + ES_TYPE_SEARCH_QUERY + "/_search")
			.withAuth(ES_USER, ES_PW, WSAuthScheme.BASIC)
			.post(queryData)
			.map { response =>
				if (response.status == 200) {

					Ok(Json.toJson((response.json \ "hits" \ "hits").as[List[JsObject]].map(hit => (hit \ "_source").as[JsObject])))
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

	// Create timeline response
	def createTimelineResponse(responseData: JsValue, queryBuilderList: ListBuffer[QueryBuilder], queryMode: String) : JsValue = {

		// Check if shard failed
		val failedCount: Int = (responseData \ "_shards" \ "failed").as[Int]

		if (failedCount > 0) {
			return Json.obj("message" -> "Elasticsearch query failed.")
		}

		// Include aggregations
		var aggList: ListBuffer[JsObject] = ListBuffer()
		var aggObject: List[JsObject] = List()
		for (qbl <- queryBuilderList) {

			// Check if aggregation contains filters
			if (!qbl.termFilters.isEmpty || (qbl.searchQuery.queryType == QUERY_TYPE_PHRASE && queryMode != QUERY_MODES(0))) {
				aggObject = (responseData \ "aggregations" \ qbl.searchQuery.idSeq \ "term_agg" \ "buckets").as[List[JsObject]]
			} else {
				aggObject = (responseData \ "aggregations" \ qbl.searchQuery.idSeq \ "buckets").as[List[JsObject]]
			} 
			
			// Create list of aggregations.
			for (bucketObject <- aggObject) {

				var keyString: String = if (queryMode != QUERY_MODES(0)) qbl.searchQuery.query else (bucketObject \ "key").as[String]
				var qMode = if (qbl.searchQuery.queryType == QUERY_TYPE_PHRASE) queryMode else null

				aggList += Json.obj(
					"key" -> keyString,
					"search_query" -> qbl.searchQuery.query,
					"type" -> qbl.searchQuery.queryType,
					"mode" -> qMode,
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

	// Create timeline query
	def createTimelineQuery(queryBuilderList: ListBuffer[QueryBuilder], analyzedQueries: ListBuffer[ListBuffer[String]], queryMode: String) : JsObject = {

		var queryFilter: ListBuffer[JsObject] = ListBuffer()
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
		var loopBreak: Boolean = false
		var analyzedQueryCounter: Int = 0
		for (qbl <- queryBuilderList) {

			if (qbl.searchQuery.query == "") {
				queryFilter = ListBuffer(Json.obj("match_all" -> Json.obj()))
				loopBreak = true
			} else if (!loopBreak) {
				if (qbl.searchQuery.queryType == QUERY_TYPE_TERM) {
					queryFilter += Json.obj("match" -> Json.obj("dokument.html" -> qbl.searchQuery.query))
				}
				else if (qbl.searchQuery.queryType == QUERY_TYPE_PHRASE) {

					if (queryMode == QUERY_MODES(0)) { // Exact

						queryFilter += Json.obj("match_phrase" -> Json.obj("dokument.html.shingles" -> qbl.searchQuery.query))

					} else if (queryMode == QUERY_MODES(3)) { // Anywhere

						queryFilter += Json.obj(
							"match" -> Json.obj(
								"dokument.html" -> Json.obj(
									"query" -> qbl.searchQuery.query, 
									"operator" -> "and"
								)
							)
						)

					} else { // Span near ordinal/not

						var ordinal = if (queryMode == QUERY_MODES(1)) false else true
						var spanTerms: ListBuffer[JsObject] = ListBuffer()
						for (st <- analyzedQueries(analyzedQueryCounter)) {
							spanTerms += Json.obj("span_term" -> Json.obj("dokument.html" -> st))
						}

						queryFilter += Json.obj(
							"span_near" -> Json.obj(
								"clauses" -> Json.toJson(spanTerms),
								"slop" -> SPAN_NEAR_SLOP_COUNT,
								"in_order" -> ordinal
							)
						)

					}

				}
				else if (qbl.searchQuery.queryType == QUERY_TYPE_WILDCARD) {
					queryFilter += Json.obj("match_phrase" -> Json.obj("dokument.html.shingles" -> qbl.searchQuery.query))
				}
			}

			analyzedQueryCounter += 1
		}
		
		// Create aggregation queries.
		analyzedQueryCounter = 0
		var aggregations: Map[String, JsObject] = Map()
		for (qbl <- queryBuilderList) {

			// Check if shingle field is required and anywhere or span near queries.
			var queryField: String = "dokument.html.shingles"
			var includeRegex: String = analyzedQueries(analyzedQueryCounter)(0)
			var excludeRegex: String = ""

			if (qbl.searchQuery.queryType == QUERY_TYPE_TERM || (qbl.searchQuery.queryType == QUERY_TYPE_PHRASE && queryMode != QUERY_MODES(0))) {
				queryField = "dokument.html"
			}

			// Create term agg and check if search term is empty
			var termAggregation = Json.obj(
				"terms" -> Json.obj(
					"min_doc_count" -> 0,
					"field" -> queryField,
					"execution_hint" -> ES_EXEC_HINT_PARAM,
					"collect_mode" -> ES_EXEC_SEARCH_PARAM,
					"include" -> includeRegex,
					"exclude" -> excludeRegex,
					"size" -> MOTIONER_AGG_MAX_COUNT
				),
				"aggs" -> yearAggregation
			)

			if (qbl.searchQuery.query == "") {
				termAggregation = Json.obj(
					"terms" -> Json.obj(
						"min_doc_count" -> 0,
						"field" -> queryField,
						"execution_hint" -> ES_EXEC_HINT_PARAM,
						"collect_mode" -> ES_EXEC_SEARCH_PARAM,
						"size" -> MOTIONER_AGG_MAX_COUNT
					),
					"aggs" -> yearAggregation
				)
			}

			// Check if there is any filter for this search.
			if (!qbl.termFilters.isEmpty || (qbl.searchQuery.queryType == QUERY_TYPE_PHRASE && queryMode != QUERY_MODES(0))) {
				var shouldFilters: ListBuffer[JsObject] = ListBuffer()
				for (tf <- qbl.termFilters) {
					if (MOTIONER_MAPPINGS_DECODER.exists(_._1 == tf.key)) {

						// Create filter object.
						var tmpFilter: JsObject = Json.obj(
							"bool" -> Json.obj(
								"should" -> Json.toJson(tf.terms.map(term => Json.obj(MOTIONER_MAPPINGS_DECODER(tf.key)._3 -> Json.obj(MOTIONER_MAPPINGS_DECODER(tf.key)._2 -> term))))
							)
						)

						// Check if filter field is nested.
						if (MOTIONER_MAPPINGS_DECODER(tf.key)._1 != null) {
							shouldFilters += Json.obj(
								"nested" -> Json.obj(
									"path" -> MOTIONER_MAPPINGS_DECODER(tf.key)._1,
									"query" -> tmpFilter
								)
							)
						} else {
							shouldFilters += Json.obj(
								"query" -> tmpFilter
							)
						}
					}
				}

				// Must filters for anywhere or near span searches
				var mustFilters: ListBuffer[JsObject] = ListBuffer()
				if (queryMode == QUERY_MODES(3)) {
					mustFilters += Json.obj(
						"match" -> Json.obj(
							"dokument.html" -> Json.obj(
								"query" -> qbl.searchQuery.query, 
								"operator" -> "and"
							)
						)
					)
				} else if (queryMode == QUERY_MODES(1) || queryMode == QUERY_MODES(2)) {
					var ordinal = if (queryMode == QUERY_MODES(1)) false else true
					var spanTerms: ListBuffer[JsObject] = ListBuffer()
					for (st <- analyzedQueries(analyzedQueryCounter)) {
						spanTerms += Json.obj("span_term" -> Json.obj("dokument.html" -> st))
					}

					mustFilters += Json.obj(
						"span_near" -> Json.obj(
							"clauses" -> Json.toJson(spanTerms),
							"slop" -> SPAN_NEAR_SLOP_COUNT,
							"in_order" -> ordinal
						)
					)
				}

				// Define aggregation object.
				aggregations += qbl.searchQuery.idSeq -> Json.obj(
					"filter" -> Json.obj(
						"bool" -> Json.obj(
							"should" -> Json.toJson(shouldFilters),
							"must" -> Json.toJson(mustFilters)
						)
					),
					"aggs" -> Json.obj(
						"term_agg" -> termAggregation
					)
				)
			} else {
				aggregations += qbl.searchQuery.idSeq -> termAggregation
			} 

			analyzedQueryCounter += 1
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

	// Create barchart response
	def createBarchartResponse(responseData: JsValue, queryBuilderList: ListBuffer[QueryBuilder], dateSpan: DateSpan, queryMode: String) : JsValue = {

		// Check if shard failed
		val failedCount: Int = (responseData \ "_shards" \ "failed").as[Int]

		if (failedCount > 0) {
			return Json.obj("message" -> "Elasticsearch query failed.")
		}

		// Include aggregations
		var aggList: ListBuffer[JsObject] = ListBuffer()
		var aggObject: List[JsObject] = List()
		for (qbl <- queryBuilderList) {

			// Check if aggregation contains filters
			if (!qbl.termFilters.isEmpty || (qbl.searchQuery.queryType == QUERY_TYPE_PHRASE && queryMode != QUERY_MODES(0))) {
				aggObject = (responseData \ "aggregations" \ qbl.searchQuery.idSeq \ "term_agg" \ "buckets").as[List[JsObject]]
			} else {
				aggObject = (responseData \ "aggregations" \ qbl.searchQuery.idSeq \ "buckets").as[List[JsObject]]
			} 
			
			// Create list of aggregations.
			for (bucketObject <- aggObject) {

				var keyString: String = if (queryMode != QUERY_MODES(0)) qbl.searchQuery.query else (bucketObject \ "key").as[String]
				var qMode = if (qbl.searchQuery.queryType == QUERY_TYPE_PHRASE) queryMode else null

				aggList += Json.obj(
					"key" -> keyString,
					"search_query" -> qbl.searchQuery.query,
					"type" -> qbl.searchQuery.queryType,
					"start_date" -> dateSpan.startDate.toString(),
					"end_date" -> dateSpan.endDate.toString(),
					"mode" -> qMode,
					"filters" -> qbl.termFilters.map(tf => Json.obj(tf.key -> tf.terms)),
					"doc_count" -> (bucketObject \ "doc_count").as[JsNumber],
					"buckets" -> (bucketObject \ "nested_agg" \ "party_agg" \ "buckets").as[JsArray]
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

	// Create barchart query
	def createBarchartQuery(queryBuilderList: ListBuffer[QueryBuilder], analyzedQueries: ListBuffer[ListBuffer[String]], dateSpan: DateSpan, queryMode: String) : JsObject = {

		var queryFilter: ListBuffer[JsObject] = ListBuffer()
		//var termAgg: List[String] = List()
		//var phraseAgg: List[String] = List()
		//var wildcardAgg: String = ""


		// Create query data object.
		var partyAggregation: JsObject = Json.obj(
			"nested_agg" -> Json.obj(
				"nested" -> Json.obj(
					"path" -> "dokintressent"
				),
				"aggs" -> Json.obj(
					"party_agg" -> Json.obj(
						"terms" -> Json.obj(
							"field" -> "dokintressent.intressent.partibet",
							"execution_hint" -> "global_ordinals_hash",
                			"collect_mode" -> "breadth_first",
                			"size" -> 10
						)
					)
				)
			)
		)

		// Date filter
		var dateFilter: JsObject = Json.obj(
			MOTIONER_MAPPINGS_DECODER("datum")._3 -> Json.obj(
				MOTIONER_MAPPINGS_DECODER("datum")._2 -> Json.obj(
					"gte" -> dateSpan.startDate.getMillis(),
					"lte" -> dateSpan.endDate.getMillis(),
					"format" -> "epoch_millis"
				)
			)
		)

		// Find query types and searches, if wildcard, ignore the rest.
		var loopBreak: Boolean = false
		var analyzedQueryCounter: Int = 0
		for (qbl <- queryBuilderList) {

			if (qbl.searchQuery.query == "") {
				queryFilter = ListBuffer(Json.obj("match_all" -> Json.obj()))
				loopBreak = true
			} else if (!loopBreak) {
				if (qbl.searchQuery.queryType == QUERY_TYPE_TERM) {
					queryFilter += Json.obj("match" -> Json.obj("dokument.html" -> qbl.searchQuery.query))
				}
				else if (qbl.searchQuery.queryType == QUERY_TYPE_PHRASE) {

					if (queryMode == QUERY_MODES(0)) { // Exact

						queryFilter += Json.obj("match_phrase" -> Json.obj("dokument.html.shingles" -> qbl.searchQuery.query))

					} else if (queryMode == QUERY_MODES(3)) { // Anywhere

						queryFilter += Json.obj(
							"match" -> Json.obj(
								"dokument.html" -> Json.obj(
									"query" -> qbl.searchQuery.query, 
									"operator" -> "and"
								)
							)
						)

					} else { // Span near ordinal/not

						var ordinal = if (queryMode == QUERY_MODES(1)) false else true
						var spanTerms: ListBuffer[JsObject] = ListBuffer()
						for (st <- analyzedQueries(analyzedQueryCounter)) {
							spanTerms += Json.obj("span_term" -> Json.obj("dokument.html" -> st))
						}

						queryFilter += Json.obj(
							"span_near" -> Json.obj(
								"clauses" -> Json.toJson(spanTerms),
								"slop" -> SPAN_NEAR_SLOP_COUNT,
								"in_order" -> ordinal
							)
						)

					}

				}
				else if (qbl.searchQuery.queryType == QUERY_TYPE_WILDCARD) {
					queryFilter += Json.obj("match_phrase" -> Json.obj("dokument.html.shingles" -> qbl.searchQuery.query))
				}
			}

			analyzedQueryCounter += 1
		}
		
		// Create aggregation queries.
		analyzedQueryCounter = 0
		var aggregations: Map[String, JsObject] = Map()
		for (qbl <- queryBuilderList) {

			// Check if shingle field is required and anywhere or span near queries.
			var queryField: String = "dokument.html.shingles"
			var includeRegex: String = analyzedQueries(analyzedQueryCounter)(0)
			var excludeRegex: String = ""

			if (qbl.searchQuery.queryType == QUERY_TYPE_TERM || (qbl.searchQuery.queryType == QUERY_TYPE_PHRASE && queryMode != QUERY_MODES(0))) {
				queryField = "dokument.html"
			}

			// Create term agg and check if search term is empty
			var termAggregation = Json.obj(
				"terms" -> Json.obj(
					"min_doc_count" -> 0,
					"field" -> queryField,
					"execution_hint" -> ES_EXEC_HINT_PARAM,
					"collect_mode" -> ES_EXEC_SEARCH_PARAM,
					"include" -> includeRegex,
					"exclude" -> excludeRegex,
					"size" -> MOTIONER_AGG_MAX_COUNT
				),
				"aggs" -> partyAggregation
			)

			if (qbl.searchQuery.query == "") {
				termAggregation = Json.obj(
					"terms" -> Json.obj(
						"min_doc_count" -> 0,
						"field" -> queryField,
						"execution_hint" -> ES_EXEC_HINT_PARAM,
						"collect_mode" -> ES_EXEC_SEARCH_PARAM,
						"size" -> MOTIONER_AGG_MAX_COUNT
					),
					"aggs" -> partyAggregation
				)
			}

			// Check if there is any filter for this search.
			if (!qbl.termFilters.isEmpty || (qbl.searchQuery.queryType == QUERY_TYPE_PHRASE && queryMode != QUERY_MODES(0))) {
				var shouldFilters: ListBuffer[JsObject] = ListBuffer()
				for (tf <- qbl.termFilters) {
					if (MOTIONER_MAPPINGS_DECODER.exists(_._1 == tf.key)) {

						// Create filter object.
						var tmpFilter: JsObject = Json.obj(
							"bool" -> Json.obj(
								"should" -> Json.toJson(tf.terms.map(term => Json.obj(MOTIONER_MAPPINGS_DECODER(tf.key)._3 -> Json.obj(MOTIONER_MAPPINGS_DECODER(tf.key)._2 -> term))))
							)
						)

						// Check if filter field is nested.
						if (MOTIONER_MAPPINGS_DECODER(tf.key)._1 != null) {
							shouldFilters += Json.obj(
								"nested" -> Json.obj(
									"path" -> MOTIONER_MAPPINGS_DECODER(tf.key)._1,
									"query" -> tmpFilter
								)
							)
						} else {
							shouldFilters += Json.obj(
								"query" -> tmpFilter
							)
						}
					}
				}

				// Must filters for anywhere or near span searches
				var mustFilters: ListBuffer[JsObject] = ListBuffer()
				if (queryMode == QUERY_MODES(3)) {
					mustFilters += Json.obj(
						"match" -> Json.obj(
							"dokument.html" -> Json.obj(
								"query" -> qbl.searchQuery.query, 
								"operator" -> "and"
							)
						)
					)
				} else if (queryMode == QUERY_MODES(1) || queryMode == QUERY_MODES(2)) {
					var ordinal = if (queryMode == QUERY_MODES(1)) false else true
					var spanTerms: ListBuffer[JsObject] = ListBuffer()
					for (st <- analyzedQueries(analyzedQueryCounter)) {
						spanTerms += Json.obj("span_term" -> Json.obj("dokument.html" -> st))
					}

					mustFilters += Json.obj(
						"span_near" -> Json.obj(
							"clauses" -> Json.toJson(spanTerms),
							"slop" -> SPAN_NEAR_SLOP_COUNT,
							"in_order" -> ordinal
						)
					)
				}

				// Define aggregation object.
				aggregations += qbl.searchQuery.idSeq -> Json.obj(
					"filter" -> Json.obj(
						"bool" -> Json.obj(
							"should" -> Json.toJson(shouldFilters),
							"must" -> Json.toJson(mustFilters)
						)
					),
					"aggs" -> Json.obj(
						"term_agg" -> termAggregation
					)
				)
			} else {
				aggregations += qbl.searchQuery.idSeq -> termAggregation
			} 

			analyzedQueryCounter += 1
		}
		

		var queryData = Json.obj(
			"size" -> 0,
			"query" -> Json.obj(
				"filtered" -> Json.obj(
					"filter" -> Json.obj(
						"bool" -> Json.obj(
							"should" -> Json.toJson(queryFilter),
							"must" -> dateFilter
						)
					)
				)
			),
			"aggs" -> Json.toJson(aggregations)
		)

		//println(Json.prettyPrint(queryData))

		return queryData

	}

	// Create hitlist response
	def createHitlistResponse(responseData: JsValue, queryBuilderList: ListBuffer[QueryBuilder], dateSpan: DateSpan, fromIndex: Int) : JsValue = {

		var hitList: List[JsObject] = (responseData \ "responses").as[List[JsObject]]
		var responseList: ListBuffer[JsObject] = ListBuffer()

		var responseCounter: Int = 0
		for (qbl <- queryBuilderList) {

			// Check if shard failed
			val failedCount: Int = (hitList(responseCounter) \ "_shards" \ "failed").as[Int]

			if (failedCount > 0) {
				responseList += Json.obj("message" -> "Elasticsearch query failed for this query.")
			} else {
				responseList += Json.obj(
					"es_query_time" -> (hitList(responseCounter) \ "took").as[JsNumber],
					"search_query" -> qbl.searchQuery.query,
					"type" -> qbl.searchQuery.queryType,
					"start_date" -> dateSpan.startDate.toString(),
					"end_date" -> dateSpan.endDate.toString(),
					"filters" -> qbl.termFilters.map(tf => Json.obj(tf.key -> tf.terms)),
					"total_hit_count" -> (hitList(responseCounter) \ "hits" \ "total").as[JsNumber],
					"from_index" -> fromIndex,
					"hits" -> (hitList(responseCounter) \ "hits" \ "hits").as[JsArray]
				)
			}

			responseCounter += 1
		}

		return Json.toJson(responseList)

	}

	// Create hitlist query
	def createHitlistQuery(queryBuilderList: ListBuffer[QueryBuilder], analyzedQueries: ListBuffer[ListBuffer[String]], dateSpan: DateSpan, fromIndex: Int, queryMode: String) : Map[String, JsObject] = {

		var queryList: Map[String, JsObject] =  Map()

		var analyzedQueryCounter: Int = 0
		for (qbl <- queryBuilderList) {

			// Check if term, phrase or wildcard.
			var matchObject: JsObject = Json.obj("match" -> Json.obj("dokument.html" -> qbl.searchQuery.query)) // Word
			if (qbl.searchQuery.query == "") { // Match all

				matchObject = Json.obj("match_all" -> Json.obj())

			} else if (qbl.searchQuery.queryType == QUERY_TYPE_WILDCARD) { // Wildcard

				matchObject = Json.obj("match_phrase" -> Json.obj("dokument.html.shingles" -> qbl.searchQuery.query))

			} else if (qbl.searchQuery.queryType == QUERY_TYPE_PHRASE) { // Phrase

				if (queryMode == QUERY_MODES(0)) { // Exact

					matchObject = Json.obj("match_phrase" -> Json.obj("dokument.html.shingles" -> qbl.searchQuery.query))

				} else if (queryMode == QUERY_MODES(3)) { // Anywhere

					matchObject = Json.obj(
						"match" -> Json.obj(
							"dokument.html" -> Json.obj(
								"query" -> qbl.searchQuery.query, 
								"operator" -> "and"
							)
						)
					)

				} else { // Span near ordinal/not

					var ordinal = if (queryMode == QUERY_MODES(1)) false else true
					var spanTerms: ListBuffer[JsObject] = ListBuffer()
					for (st <- analyzedQueries(analyzedQueryCounter)) {
						spanTerms += Json.obj("span_term" -> Json.obj("dokument.html" -> st))
					}

					matchObject = Json.obj(
						"span_near" -> Json.obj(
							"clauses" -> Json.toJson(spanTerms),
							"slop" -> SPAN_NEAR_SLOP_COUNT,
							"in_order" -> ordinal
						)
					)

				}

			}

			// Add term filters.
			var filters: ListBuffer[JsObject] = ListBuffer()
			for (tf <- qbl.termFilters) {

				if (MOTIONER_MAPPINGS_DECODER.exists(_._1 == tf.key)) {

					// Create filter object.
					var tmpFilter: JsObject = Json.obj(
						"bool" -> Json.obj(
							"should" -> Json.toJson(tf.terms.map(term => Json.obj(MOTIONER_MAPPINGS_DECODER(tf.key)._3 -> Json.obj(MOTIONER_MAPPINGS_DECODER(tf.key)._2 -> term))))
						)
					)

					// Check if filter field is nested.
					if (MOTIONER_MAPPINGS_DECODER(tf.key)._1 != null) {
						filters += Json.obj(
							"nested" -> Json.obj(
								"path" -> MOTIONER_MAPPINGS_DECODER(tf.key)._1,
								"query" -> tmpFilter
							)
						)
					} else {
						filters += Json.obj(
							"query" -> tmpFilter
						)
					}
				}
				
			}

			// Add date filters.
			filters += Json.obj(
				MOTIONER_MAPPINGS_DECODER("datum")._3 -> Json.obj(
					MOTIONER_MAPPINGS_DECODER("datum")._2 -> Json.obj(
						"gte" -> dateSpan.startDate.getMillis(),
						"lte" -> dateSpan.endDate.getMillis(),
						"format" -> "epoch_millis"
					)
				)
			)

			queryList += qbl.searchQuery.idSeq ->Json.obj(
				"from" -> fromIndex,
				"size" -> MOTIONER_HIT_RETURN_COUNT,
				"query" -> Json.obj(
					"filtered" -> Json.obj(
						"query" -> matchObject
					)
				),
				"filter" -> Json.obj(
					"bool" -> Json.obj(
						"must" -> Json.toJson(filters)
					)
				)
			)

			analyzedQueryCounter += 1

		}

		//for (qbl <- queryBuilderList) {
		//	println(Json.prettyPrint(queryList(qbl.searchQuery.idSeq)))
		//}

		return queryList

	}

	// Store information about search queries and filters in ES.
	def storeSearchQueries(queryBuilderList: ListBuffer[QueryBuilder]) = Future {

		val ES_INDEX_QUERY = Play.current.configuration.getString("es.queries.index").get
		val ES_TYPE_SEARCH_QUERY = Play.current.configuration.getString("es.queries.type.search_query").get

		var currentDate: DateTime = new DateTime()
		var dateFormatter: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

		for (qbl <- queryBuilderList) {

			var tmpSearchQuery: String = qbl.searchQuery.query
			if (tmpSearchQuery.contains(QUERY_WILDCARD_ES)) {
				tmpSearchQuery = tmpSearchQuery.replace(QUERY_WILDCARD_ES, QUERY_WILDCARD_SUB)
			}

			// Store search query in ES.
			var queryData = Json.obj(
				"term" -> tmpSearchQuery,
				"type" -> qbl.searchQuery.queryType,
				"date" -> dateFormatter.print(currentDate)
			)
			
			WS.url(ES_HOST + HOST_URL_APPEND + ES_INDEX_QUERY + "/" + ES_TYPE_SEARCH_QUERY)
				.withAuth(ES_USER, ES_PW, WSAuthScheme.BASIC)
				.post(queryData)

		}

	}
	
	// Fetch analyzed string
	def fetchAnalyzedQuery(queryBuilder: QueryBuilder, queryMode: String) : Future[ListBuffer[String]] = {

		var analyzerType = "custom_shingle_analyzer"
		if (queryBuilder.searchQuery.queryType == QUERY_TYPE_TERM || (queryBuilder.searchQuery.queryType == QUERY_TYPE_PHRASE && queryMode != QUERY_MODES(0))) {
			analyzerType = "custom_html_analyzer"
		}

		var queryData = Json.obj(
			"analyzer" -> analyzerType,
			"text" -> queryBuilder.searchQuery.query
		)

		// Analyze search query
		val futureResult: Future[ListBuffer[String]] = WS.url(ES_HOST + HOST_URL_APPEND + ES_INDEX + "/_analyze")
			.withAuth(ES_USER, ES_PW, WSAuthScheme.BASIC)
			.post(queryData)
			.map { response =>
				if (response.status == 200) {

					if (queryBuilder.searchQuery.queryType == QUERY_TYPE_TERM || (queryBuilder.searchQuery.queryType == QUERY_TYPE_PHRASE && queryMode != QUERY_MODES(0))) {
						var tokens: ListBuffer[String] = ListBuffer()
						for (token <- (response.json \ "tokens").as[List[JsObject]]) {
							tokens += (token \ "token").as[String]
						}

						tokens
					} else {
						var longestToken: String = ""
						for (token <- (response.json \ "tokens").as[List[JsObject]]) {
							if ((token \ "token").as[String].length() > longestToken.length()) {
								longestToken = (token \ "token").as[String]
							}
						}

						var wildCardIndex: Int = queryBuilder.searchQuery.query.indexOfSlice(QUERY_WILDCARD_ES)
						if (wildCardIndex > -1) {
							val (fst, snd) = longestToken.splitAt(wildCardIndex)
							longestToken = fst.trim() + queryBuilder.searchQuery.query.slice(wildCardIndex - 1, wildCardIndex + QUERY_WILDCARD_ES.length() + 1) + snd.trim()
						}

						ListBuffer(longestToken)
					}
				} else {
					ListBuffer(queryBuilder.searchQuery.query)
				}
				
			}

		futureResult

	}
	
	// Parser for search queries
	def queryParser(searchPhrase: String) : ListBuffer[QueryBuilder] = {

		// Declare return object
		var queryBuilderList: ListBuffer[QueryBuilder] = ListBuffer()

		// Split search terms
		var splitResult: ListBuffer[String] = ListBuffer()
		var startIndex: Int = 0
		var currIndex: Int = 0
		var inParentheses: Boolean = false
		for (c <- searchPhrase) {

			if (c == '(') inParentheses = true
			else if (c == ')') inParentheses = false

			if (currIndex == (searchPhrase.length() - 1)) {
				splitResult += searchPhrase.substring(startIndex).trim()
			} else if (c == ',' && !inParentheses) {
				splitResult += searchPhrase.substring(startIndex, currIndex).trim()
				startIndex = currIndex + 1
			}

			currIndex += 1

		}

		if (splitResult.isEmpty) {
			queryBuilderList += QueryBuilder(SearchQuery(QUERY_TYPE_TERM, "", randomAlphanumericString(10)), ListBuffer())
		} else {

			// Iterate future buckets
			for (sr <- splitResult) {
				
				var searchQueries: ListBuffer[SearchQuery] = ListBuffer()
				var termFilters: ListBuffer[TermFilter] = ListBuffer()

				// Parse splitted term
				var pattern = "(\\S*?):\\(.*?\\)".r
				val filterExtractions = pattern.findAllIn(sr)
				val searchQuery = pattern.replaceAllIn(sr, "")

				// Parse search query
				var wordCount: Int = searchQuery.split(" ").length
				if (searchQuery == "") {
					searchQueries += SearchQuery(QUERY_TYPE_TERM, "", randomAlphanumericString(10))
				} else if (searchQuery.contains('(') && searchQuery.contains(')')) { // Multiple terms
					for (sq <- searchQuery.replace('(', ' ').replace(')', ' ').split(",").toList) {
						searchQueries += SearchQuery(QUERY_TYPE_TERM, sq.trim(), randomAlphanumericString(10))
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

							searchQueries += SearchQuery(QUERY_TYPE_WILDCARD, searchQuery.split(" ").slice(startSlice, stopSlice).mkString(" ").replace(QUERY_WILDCARD_SUB, QUERY_WILDCARD_ES).trim(), randomAlphanumericString(10))

						} else searchQueries += SearchQuery(QUERY_TYPE_WILDCARD, searchQuery.replace(QUERY_WILDCARD_SUB, QUERY_WILDCARD_ES).trim(), randomAlphanumericString(10))

					} else if (wordCount > 1) { // Phrase
						if (wordCount > MAX_SHINGLE_SIZE) searchQueries += SearchQuery(QUERY_TYPE_PHRASE, searchQuery.split(" ").take(MAX_SHINGLE_SIZE).mkString(" ").trim(), randomAlphanumericString(10))
						else searchQueries += SearchQuery(QUERY_TYPE_PHRASE, searchQuery.trim(), randomAlphanumericString(10))

					} else { // Term
						searchQueries += SearchQuery(QUERY_TYPE_TERM, searchQuery.trim(), randomAlphanumericString(10))
					}
				}

				// Parse filters
				for(fe <- filterExtractions) {

					var filterKey: Option[String] = "(.*?)(?=:)".r.findFirstIn(fe.trim)
					var filterParams: Option[String] = "(?<=\\()(.*?)(?=\\))".r.findFirstIn(fe.trim)

					// Check if filter key and params are found
					if (filterKey != None && filterParams != None) {
						termFilters += TermFilter(filterKey.get, filterParams.get.split(",").map(_.trim).toList)
					}

				}

				// Insert parsed data into query builder object
				for (sq <- searchQueries) {
					queryBuilderList += QueryBuilder(sq, termFilters)
				}

			}
		}

		return queryBuilderList

	}

	def checkQueryMode(queryMode: String) : String = {
		if (QUERY_MODES.contains(queryMode)) {
			return queryMode
		} else {
			return QUERY_MODES(0)
		}
	}

	// Parse start and end date.
	def dateParser(startDate: String, endDate: String) : DateSpan = {

		var dateParsers: Array[DateTimeParser] = Array(
			DateTimeFormat.forPattern("yyyyMMdd").getParser(),
			DateTimeFormat.forPattern("yy").getParser(),
			DateTimeFormat.forPattern("yyyy").getParser()
		)

		var dateFormatter: DateTimeFormatter = new DateTimeFormatterBuilder().append(null, dateParsers).toFormatter();
		var startDateObject: DateTime = dateFormatter.parseDateTime(startDate)
		var endDateObject: DateTime = dateFormatter.parseDateTime(endDate).plusYears(1)

		return DateSpan(startDateObject, endDateObject)

	}

	// Generate a random string of length n from the given alphabet
	def randomString(alphabet: String)(n: Int): String = 
		Stream.continually(random.nextInt(alphabet.size)).map(alphabet).take(n).mkString

	// Generate a random alphabnumeric string of length n
	def randomAlphanumericString(n: Int) = 
		randomString("abcdefghijklmnopqrstuvwxyz0123456789")(n)

}