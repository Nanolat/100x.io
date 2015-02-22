package io.x100.colstore.store

import io.x100.colstore.{TableScanOrder, TRow}

import scala.annotation.tailrec

object StoreMerger {
  def fromArray(stores : Array[TStoreReader]) : StoreMerger = {
    assert(stores.length >= 2)

    if ( stores.length == 2) {
      new StoreMerger( stores(0), stores(1) )
    } else {
      new StoreMerger( stores(0), fromArray(stores.drop(1)) )
    }
  }
}

/**
 * Created by unknown on 2/19/15.
 */
class StoreMerger(left : TStoreReader, right : TStoreReader) extends TStoreReader {
  def get(key : Long) : TRow = {
    // TODO : implement
    assert(false)
    null
  }
  def scan(key : Option[Long], order : TableScanOrder.TableScanOrder) : Stream[TRow] = {
    // TODO : implement
    assert(false)
    null
  }
}
