package chap05

import common.PathUtil
import org.junit.Test

class RelativeFrequencyScalaTest {

  @Test
  def test(): Unit = {
    RelativeFrequencyScala.main(Array(
      "2",
      PathUtil.getInputPath("sample_input_word.txt"),
      PathUtil.getOutputPath("tmp")
    ))
  }

  @Test
  def test1(): Unit = {
    SparkSQLRelativeFrequencyScala.main(Array(
      "2",
      PathUtil.getInputPath("sample_input_word.txt"),
      PathUtil.getOutputPath("tmp")
    ))
  }
}
