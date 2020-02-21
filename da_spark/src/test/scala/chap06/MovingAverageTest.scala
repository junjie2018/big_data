package chap06

import common.PathUtil
import org.junit.Test

class MovingAverageTest {
  @Test
  def test(): Unit = {
    MovingAverageInMemory.main(Array(
      "3",
      PathUtil.getInputPath("sample_input_average.txt"),
      PathUtil.getOutputPath("tmp")
    ))
  }

  @Test
  def test2(): Unit = {
    MovingAverage.main(Array(
      "3",
      "2",
      PathUtil.getInputPath("sample_input_average.txt"),
      PathUtil.getOutputPath("tmp")
    ))
  }
}
