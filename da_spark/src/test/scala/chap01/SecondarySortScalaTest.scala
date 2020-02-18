package chap01

import common.PathUtil
import org.junit.Test

class SecondarySortScalaTest {

  @Test
  def testSecondarySortTest(): Unit = {
    val paths = PathUtil.getPath("sample_input.txt", "tmp4")
    val args = Array("1")

    val newArgs = args ++ paths

    SecondarySort.main(newArgs)
  }
}
