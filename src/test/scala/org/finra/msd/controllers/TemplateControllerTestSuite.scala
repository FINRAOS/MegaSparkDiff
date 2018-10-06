package org.finra.msd.controllers

import org.scalatest.FunSuite

class TemplateControllerTestSuite extends FunSuite {

  test("html template loading")
  {
    val html = TemplateController.horizontalTableTemplate
    assert(html.contains("</html>"))
  }
}
