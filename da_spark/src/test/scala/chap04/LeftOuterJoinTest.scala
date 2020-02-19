package chap04

import common.PathUtil
import org.junit.Test

class LeftOuterJoinTest {
  @Test
  def test1(): Unit = {
    LeftOuterJoin.main(Array(
      PathUtil.getInputPath("user.txt"),
      PathUtil.getInputPath("transactions.txt"),
      PathUtil.getOutputPath("tmp")))
  }

  @Test
  def test2(): Unit = {
    SparkLeftOuterJoinScala.main(Array(
      PathUtil.getInputPath("user.txt"),
      PathUtil.getInputPath("transactions.txt"),
      PathUtil.getOutputPath("tmp")))
  }

}
