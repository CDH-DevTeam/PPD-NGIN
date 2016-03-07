package controllers

import play.api._
import play.api.mvc._
import play.api.libs.json._
import play.api.libs.ws._
import play.api.Play.current
import play.api.libs.concurrent.Execution.Implicits._

import scala.concurrent.{Await, Future}
import scala.util.matching.Regex

class Application extends Controller {

	/*
	 *	Configuration variables
	 */

	val es_host = Play.current.configuration.getString("es.host").get
	val es_index = Play.current.configuration.getString("es.index").get
	val es_user = Play.current.configuration.getString("es.username").get
	val es_pw = Play.current.configuration.getString("es.password").get
	
	/*
	 *	Actions
	 */

	// Index page
	def index = Action {
		Ok(views.html.index("Your new application is ready."))
	}

	// Get information from ES index about types and counts.
	def get_doc_count(doc_type: String) = Action.async {

		WS.url(es_host + es_index + "/" + doc_type + "/_count")
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

	
	def get_motioner_timeline(search_phrase: String) = Action.async {

		// Set variables
		val es_type = Play.current.configuration.getString("es.type.motioner").get

		// Parse search string
		val parsed = query_parser(search_phrase)

		val match_tags: List[String] = search_phrase.split(",").map(_.trim).toList
		println(match_tags)

		Future {
			Thread.sleep(1000)
			"tjenare mannen"
		}.map {
			res => Ok(res)
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
		WS.url(es_host + es_index + "/" + es_type + "/_search")
			.withAuth(es_user, es_pw, WSAuthScheme.BASIC)
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
	}
	
	/*
	 *	Help Functions
	 */

	def query_parser(search_phrase: String) : String = {

		println(" ")
		println(search_phrase)

		val str = "Scala is scalable and cool try:(test, asdf) filter:(1244-2345)"

		// Extract filter from search phrase
		var pattern = "(\\S*?):\\(.*?\\)".r
		val filter_extractions = pattern.findAllIn(str)
		val remaining_terms = pattern.replaceAllIn(str, "")

		println(remaining_terms)

		// Parse filter terms
		for(filter <- filter_extractions) {

			var key: Option[String] = "(.*?)(?=:)".r.findFirstIn(filter.trim)
			var filter_params: Option[String] = "(?<=\\()(.*?)(?=\\))".r.findFirstIn(filter.trim)

			// If filter key and params are found
			if (key != None && filter_params != None) {
				println(key.get)
				println(filter_params.get)

				// Check if filter params contain year segement or string filters
				if (filter_params.get.contains("-")) {
					var start_year: Option[String] = "(\\d*?)(?=\\-)".r.findFirstIn(filter_params.get)
					var end_year: Option[String] = "(?<=\\-)(\\d*?)($|\\,|\\s)".r.findFirstIn(filter_params.get)
					
					if (start_year != None && end_year != None) {
						println(start_year.get.toInt)
						println(end_year.get.toInt)
					}

				} else {

				}
			}
		}

		return "test"
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

	WS.url(es_host + "ppd/type_anforande/_seach?pretty=true")
		.post(data)
		.map { response =>
			Ok(response.body)
		}

}

*/