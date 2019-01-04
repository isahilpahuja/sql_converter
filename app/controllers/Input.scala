package controllers

import play.api.libs.json.Json



case class Input(databaseName: String,sqlQuery: String,sparkCode: String)

object Input {

  implicit val format = Json.format[Input]

}
