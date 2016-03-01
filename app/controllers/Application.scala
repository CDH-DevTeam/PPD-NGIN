package controllers

import play.api._
import play.api.mvc._
import play.api.libs.json._
//import play.api.libs.functional.syntax._
import play.api.libs.ws._
//import play.api.libs.ws.ning.NingAsyncHttpClientConfigBuilder
import play.api.Play.current
import play.api.libs.concurrent.Execution.Implicits._

import scala.concurrent.{Await, Future}

class Application extends Controller {

	// Get configuration settings
	val es_host = Play.current.configuration.getString("es.host").get
	val es_index = Play.current.configuration.getString("es.index").get
	
	// Index page
	def index = Action {
		Ok(views.html.index("Your new application is ready."))
	}

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


	def get_motioner_all() = Action {
		print("get motioner all")
		Ok("Get all motioner")
	}

	
	def get_motioner_from_keywords(tags: String) = Action.async {

		
		val es_type = Play.current.configuration.getString("es.type.motioner").get

		/*
		if (tags.isEmpty) {
			Redirect(routes.Application.get_motioner_all())
		} else {
		*/

		val match_tags: List[String] = tags.split(",").map(_.trim).toList

		val data = Json.obj(
			"query" -> Json.obj(
				"match" -> Json.obj(
					"dokument.html.html_filtered" -> match_tags.mkString(" ")
				)
			),
			"fields" -> Json.arr(
				"dokument.dokument_url_html"
			)
		)

		println(data)

		WS.url(es_host + es_index + "/" + es_type + "/_search")
			.post(data)
			.map { response =>
				//println(response.status)
				//println(response.body)
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
	

}





/*

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
