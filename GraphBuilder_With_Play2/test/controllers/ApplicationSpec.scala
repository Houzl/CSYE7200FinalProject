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
import play.api.libs.ws.WS

class ApplicationSpec extends PlaySpec with OneAppPerTest{

  "run in a server" in {
    running(TestServer(8000)) {

      await(WS.url("http://localhost:8000").get).status mustBe OK

    }
  }
}
