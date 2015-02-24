package io.x100.colstore.index

import org.scalatest.{Suite, BeforeAndAfterEach}

trait SortedArrayInstanceTrait extends BeforeAndAfterEach {
  this: Suite =>
  val keySpaceSize = 6
  val keyLength = 2
  var sarray: SortedArray = null

  def newArray = new SortedArray(keySpaceSize = 6, keyLength = 2)

  override def beforeEach() {
    // set-up code
    //
    sarray = newArray

    super.beforeEach();
  }

  override def afterEach() {
    super.beforeEach();
    // tear-down code
    //
    sarray = null
  }
}
