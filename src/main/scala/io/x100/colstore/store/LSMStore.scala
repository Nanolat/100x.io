package io.x100.colstore.store

import io.x100.colstore.{ColumnData, TableScanOrder, TRow}

/**
 * Created by unknown on 2/19/15.
 */
class LSMStore(val mutableStore_ : TMutableStore) {
  def put(tableId : Long, key : Long, columns : Array[ColumnData]) : Unit = {
    mutableStore_.put(key, columns)
  }

  def del(tableId : Long, key : Long) : Unit = {
    mutableStore_.del(key)
  }

  def get(tableId : Long, key : Long) : TRow = {
    // TODO : Implement
    assert(false)
    null
  }

  def scan(tableId : Long, key : Option[Long], order : TableScanOrder.TableScanOrder) : Stream[TRow] = {
    // TODO : Implement
    assert(false)
    null
  }
}
