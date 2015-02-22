package io.x100.colstore.store

import io.x100.colstore.{TableScanOrder, TRow, ColumnData}

/**
 * Created by unknown on 2/19/15.
 */
trait TMutableStore {
  def put(key : Long, columns : Array[ColumnData]) : Unit
  def del(key : Long) : Unit
  def clear() : Unit
}

class MemStore extends TMutableStore with TStoreReader {
  val keyTree_ = new java.util.TreeMap[Long, Array[ColumnData]]()

  def put(key : Long, columns : Array[ColumnData]) : Unit = {
    // BUGBUG : Thread safety!
    keyTree_.put(key, columns)
  }
  def del(key : Long) : Unit = {
    // BUGBUG : Thread safety!
    // An empty array for the value means that the key was deleted.
    keyTree_.put(key, Array())
  }
  def clear() : Unit = {
    // BUGBUG : Thread safety!
    keyTree_.clear()
  }

  def get(key : Long) : TRow = {
    // TODO : Implement
    assert(false)
    null
  }

  def scan(key : Option[Long], order : TableScanOrder.TableScanOrder) : Stream[TRow] = {
    // TODO : Implement
    assert(false)
    null
  }
}