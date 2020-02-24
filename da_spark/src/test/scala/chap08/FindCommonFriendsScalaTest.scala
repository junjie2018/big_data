package chap08

import common.PathUtil
import org.junit.Test

class FindCommonFriendsScalaTest {
  @Test
  def test(): Unit = {
    FindCommonFriendsScala.main(Array(
      PathUtil.getInputPath("sample_input_friends.txt"),
      PathUtil.getOutputPath("tmp2")
    ))
  }
}
