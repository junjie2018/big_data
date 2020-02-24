package chap07

import common.PathUtil
import org.junit.Test

class FindAssociationRulesScalaTest {
  @Test
  def test(): Unit = {
    FindAssociationRulesScala.main(Array(
      PathUtil.getInputPath("sample_input_shoppingcat.txt"),
      PathUtil.getOutputPath("tmp")
    ))
  }
}
