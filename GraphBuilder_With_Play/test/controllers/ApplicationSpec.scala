package controllers


import org.junit.runner._
import org.scalatest.junit.JUnitRunner
import org.specs2.matcher._
import org.specs2.mutable._
import org.specs2.runner._
import play.api.test.Helpers._
import play.api.test._
import play.test.WithApplication
import org.scalatestplus.play._

class ApplicationSpec extends PlaySpec with OneAppPerTest{

  "Application" should {

    "respond to the search Action" in {
      val result = controllers.Application.searchChild("1")(FakeRequest())

      status(result) mustBe OK
    }

  }
}
