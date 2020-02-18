package chap03

import common.PathUtil
import org.junit.Test

class TopNTest {

  @Test
  def test1(): Unit = {
    TopN.main(PathUtil.getPath("sample_input_cat.txt", ""))
  }

  @Test
  def test2(): Unit = {
    TopNNonUnique.main(PathUtil.getPath("sample_input_url.txt", ""))
  }
}
