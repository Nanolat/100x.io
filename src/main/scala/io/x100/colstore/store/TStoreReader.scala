package io.x100.colstore.store

import io.x100.colstore.{TableScanOrder, TRow}

/**
 * Created by unknown on 2/19/15.
 */

trait TStoreReader {
  def get(key : Long) : TRow
  def scan(key : Option[Long], order : TableScanOrder.TableScanOrder) : Stream[TRow]
}

class FileStore extends TStoreReader {
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
