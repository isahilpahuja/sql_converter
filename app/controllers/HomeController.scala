package controllers

import javax.inject._
import play.api._
import play.api.mvc._
import play.twirl.api.StringInterpolation

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class HomeController @Inject()(cc: ControllerComponents) extends AbstractController(cc) {

  /**
   * Create an Action to render an HTML page.
   *
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def main() = Action { implicit request: Request[AnyContent] =>
    Ok(views.html.index())
  }

  def read(id: String) = Action { implicit request: Request[AnyContent] => {
    if (id.equalsIgnoreCase("play")) {
      Ok("SQL->"+id)
      //@if(true) {
     /* val name = "Martin"
      html"<p>Hello $name</p>"*/
        //<h1>Nothing to display</h1>
      //}
     // Ok(views.html.index())
    }
    else {
      Ok("else SQL->"+id)
      //Ok(views.html.test())
    }

  }
    /*=>
    widgetRepo.select(BSONDocument(Id -> BSONObjectID(id))).map(widget => Ok(Json.toJson(widget)))*/



    /*=>
    widgetRepo.select(BSONDocument(Id -> BSONObjectID(id))).map(widget => Ok(Json.toJson(widget)))*/
  }

}
