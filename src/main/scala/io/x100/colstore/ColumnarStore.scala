package io.x100.colstore

/**
 * Describe.
 *
 * @param p Describe.
 * @author      Kangmo Kim <kangmo.kim@gmail.com>
 */


/**
 * Describe.
 *
 * @param p Describe.
 * @return r Describe.
 */


/**
 * An exception that is thrown when the iteration on a cursor finished.
 */
class OutOfCursorFenceException extends Exception

/**
 * An exception that is thrown when the specified value on TRow.scan is invalid.
 */
class OutOfValueIndexException extends Exception


/**
 * A case class that represents the data kept on a specific column.
 *
 * @param columnIndex The index of a column, starts with 0.
 * @param longStream The data that is kept on the column. It is a stream of long values.
 * @author      Kangmo Kim <kangmo.kim@gmail.com>
 */
case class ColumnData(columnIndex : Int, longStream : Stream[Long])

/**
 * A row that has access to column data.
 *
 * @author      Kangmo Kim <kangmo.kim@gmail.com>
 */
trait TRow {
  /**
   * Get the key of the row.
   *
   * @return A Long value representing the key of the row.
   */
  def getKey() : Long

  /**
   * Get column data of a specific column on the row.
   *
   * @param columnIndex The index of the column to get the column data.
   * @return The ColumnData.
   */
  def getColumn(columnIndex : Int) : ColumnData

  /**
   * Scan values on the column data in ascending order from a specific value index.
   * Ex> In case a column has values [1001,1002,1003,1004], and the scan starts from value index 2,
   * The scanned values are 1003 and 1004.
   *
   * @param valueIndex The index of the value where the scan starts.
   * @return A stream to traverse values from the searched position.
   * @throws OutOfValueIndexException if the specified valueIndex is not within a valid range.
   */
  def scanValues(valueIndex : Int) : Stream[Long]
}

/**
 * An enumeration that describes the scan order. Either ASCENDING or DESCENDING.
 */
object TableScanOrder extends Enumeration {
  type TableScanOrder = Value
  val ASCENDING, DESCENDING = Value
}

/**
 * A columnar table that keeps data for each column to efficiently compress data.
 * The table keeps rows in the order of keys, so that one can traverse rows either in ascending or descending order.
 * A column on a row can have multiple values.
 *                                     .
 *                     .-------------------.-------------------.
 *                     |     Column 0      |     Column 1      |
 *                     | (Physical Layout) | (Physical Layout) |
 *                     |                   |                   |
 *     .-----------.   |                   |                   |
 *     | Row Key 0 |--------------------------------.          |
 *     '-----------'   |        |          |        |          |
 *                     |        v          |        v          |
 *                     |  .-----------.    |  .-----------.    |
 *                     |  |  Value 0  |    |  |  Value 0  |    |
 *                     |  '-----------'    |  '-----------'    |
 *                     |  .-----------.    |  .-----------.    |
 *                     |  |  Value 1  |    |  |  Value 1  |    |
 *                     |  '-----------'    |  '-----------'    |
 *                     |                   |  .-----------.    |
 *                     |                   |  |  Value 2  |    |
 *                     |                   |  '-----------'    |
 *     .-----------.   |                   |                   |
 *     | Row Key 1 |--------------------------------.          |
 *     '-----------'   |        |          |        |          |
 *                     |        v          |        v          |
 *                     |  .-----------.    |  .-----------.    |
 *                     |  |  Value 0  |    |  |  Value 0  |    |
 *                     |  '-----------'    |  '-----------'    |
 *                     |  .-----------.    |                   |
 *                     |  |  Value 1  |    |                   |
 *                     |  '-----------'    |                   |
 *                     '-------------------'-------------------'
 *
 * @author      Kangmo Kim <kangmo.kim@gmail.com>
 */

trait TTable {
  /**
   * Put a key with a row, which has an array of columns. Replace columns if it already exists.
   * When replacing existing columns, ones that are not specified on the columns parameter are kept unchanged.
   *
   * For a document store, the key is simply a dummy value such as 0 and
   * the column 0 will have a list of Huffman codes for each term in the document.
   * ( After processing stopping and stemming on the document, the result terms are stored on the document store. )
   * When creating a document snippet on the search result, you can get a row of the document by invoking get(0) to get TRow.
   * And then, you can scan values on the column starting from a specific position on the document
   * by invoking TRow.scanValues. In this case, each value is Huffman code, so we can lookup code table
   * to get the actual word to show on the document snippet.
   *
   * For an inverted index, each term is mapped to a TTable and a row on TTable is a posting record.
   * We have two types of inverted indexes, where the key of a posting record can be either document id(=Table id) or
   * score of the document for the ranking.
   *
   * Once we receive a query, we process the same stopping and stemming on the query terms, and search the inverted index as shown below.
   *
   * 1. Queries with only one term.
   *   In this case, the key of the TTable is the score of a document and the first value of column 0 is document id.
   *   For single term query, we can simply get the document ranking
   *   by scanning rows(=postings) on the TTable in descending order of the key(=score of each document).
   *
   * 2. Queries with more than two terms.
   *   For multi-term queries, we calculate the score of each document using term-at-a-time method.
   *   In this case, document id is the key of TTable, and values on TRow has positions on the document where the term occurs.
   *   The position is 0-based index on the values of TRow on the document store.
   *   Because merging posting records of multiple terms is expensive, we will have multi-term cache table,
   *   which has the score of a document as a key, and document id as the first value of column 0.
   *   ( We will need a map from an ordered list of multiple terms to table id of the TTable that has scores and document ids.)
   *
   *
   * @param key The key of a row to put.
   * @param columns An array of (column index, column data) pair.
   */
  def put(key : Long, columns : Array[ColumnData]) : Unit

  /**
   * Get all columns on a row specified by a key.
   *
   * @param key The key of the row to get.
   * @return A row that has access to an array of column data.
   */
  def get(key : Long) : TRow

  /**
   * Scan rows on the table either in ascending order or descending order.
   *
   * @param key The key of the row where the scan starts.
   *            If None, the scan starts from the maximum key(order=DESCENDING) or minimum key(order=ASCENDING).
   * @param order The order to scan. Either ASCENDING or DESCENDING.
   * @return A row stream to traverse rows from the searched position.
   */
  def scanRows(key : Option[Long], order : TableScanOrder.TableScanOrder) : Stream[TRow]

  /**
   * Delete a row specified by a key.
   *
   * @param key The key of the row to delete.
   */
  def del(key : Long) : Unit
}

/**
 * A columnar store that manages multiple columnar tables.
 * It is used to implement the document store and inverted index.
 * The store should be able to open a table by id in O(Log N).
 * After opening a table, you can insert a key with multiple columns.
 * Each column can have multiple values of Long type which is read from a stream.
 * The table keeps data for each column to compress the data using delta encoding, bitmaps, etc.
 *
 * For a document store, the id of a table is the document id.
 * For an inverted index, the id of a table is Huffman code of each word in a lexicon.
 * ( According to Zipf's law, we will have only small number of words that are frequently used,
 *   so the more the term exists, use the less amount of bits to encode it. )
 *
 * Ex> The word 'the' will occur in almost all documents, so we encode it with 2~3 bits only.
 *     The word 'pneumonia' will occur only for documents related to medical subject, so we encode it with 20~30 bits.
 *
 * @author      Kangmo Kim <kangmo.kim@gmail.com>
 */
trait TColumnarStore {

  /**
   * Opens a columnar table.
   *
   * Use {@link #closeTable(table : TTable)} to close the opened table.
   *
   * @param id The id of the table to open.
   * @return TTable The opened table. Create a new one if a table associated with the id does not exist yet.
   */
  def openTable(id : Long) : TTable

  /**
   * Closes an open table .
   *
   * Use {@link #openTable(id : Long)} to open a table.
   *
   * @param table The table to be closed.
   */
  def closeTable(table : TTable) : Unit
}