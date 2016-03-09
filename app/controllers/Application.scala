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

		/*
		// Create ES queries
		val data = Json.obj(
			"query" -> Json.obj(
				"match" -> Json.obj(
					"dokument.html" -> match_tags.mkString(" ")
				)
			)
		)

		println(data)

		// Query ES
		WS.url(ES_HOST + ES_INDEX + "/" + es_type + "/_search")
			.withAuth(ES_USER, ES_PW, WSAuthScheme.BASIC)
			.post(data)
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
		*/

		Future {
			Thread.sleep(1000)
			"tjenare mannen"
		}.map {
			res => Ok(res)
		}
	}
	
	/*
	 *	Help Functions
	 */

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
	/*
	def query_parser(search_phrase: String) : QueryBuilder = {

		// Extract filter from search phrase
		var pattern = "(\\S*?):\\(.*?\\)".r
		val filter_extractions = pattern.findAllIn(search_phrase)
		val search_filter = pattern.replaceAllIn(search_phrase, "")


		//var date_filters: List[(String, Int, Int)] = List()
		var date_filters: List[DateFilter] = List()
		var word_filters: List[WordFilter] = List()

		// Parse filter terms
		for(filter <- filter_extractions) {

			var key: Option[String] = "(.*?)(?=:)".r.findFirstIn(filter.trim)
			var filter_params: Option[String] = "(?<=\\()(.*?)(?=\\))".r.findFirstIn(filter.trim)

			// If filter key and params are found
			if (key != None && filter_params != None) {

				// Check if filter params contain year segment or string filters
				if (filter_params.get.contains("-")) {
					var start_year: Option[String] = "(\\d*?)(?=\\-)".r.findFirstIn(filter_params.get)
					var end_year: Option[String] = "(?<=\\-)(\\d*?)($|\\,|\\s)".r.findFirstIn(filter_params.get)
					
					if (start_year != None && end_year != None) {

						date_filters = DateFilter(key.get, start_year.get.toInt, end_year.get.toInt) :: date_filters
					}

				} else {
					word_filters = WordFilter(key.get, filter_params.get.split(",").map(_.trim)) :: word_filters
				}
			}
		}

		return QueryBuilder(
			search_filter,
			date_filters,
			word_filters
		)
	}
	*/

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