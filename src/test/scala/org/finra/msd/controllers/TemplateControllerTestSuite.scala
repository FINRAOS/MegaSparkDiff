package org.finra.msd.controllers

import org.scalatest.FunSuite

class TemplateControllerTestSuite extends FunSuite {

  test("horizontalTableTempplateLocation Test")
  {
    val path = TemplateController.horizontalTableTemplateLocation
    assert(path.contains("horizontalTableTemplate.html"))
  }

  test("html template loading")
  {
    val html = TemplateController.horizontalTableTemplate
    assert(html.contains("</html>"))
  }
}
