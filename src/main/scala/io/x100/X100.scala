package io.x100

/**
 * Momdero API runs on top of ranking and snippet generator layer.
 * These again use inverted index and document store layer.
 * For efficient processing of inverted index and document store, Momdero uses
 * Columnar storage engine, which stores data for each column to compress data using delta encoding, bitmaps, etc.
 *
 *     .-------------------------------------.
 *     | Momdero API                         |
 *     '-------------------------------------'
 *     .----------------. .------------------.
 *     | Ranking        | | SnippetGenerator |
 *     '----------------' '------------------'
 *     .----------------. .------------------.
 *     | Inverted Index | | Document Store   |
 *     '----------------' '------------------'
 *     .-------------------------------------.
 *     | Columnar Storage                    |
 *     '-------------------------------------'
 *
 * Following sequence diagram depicts term-at-a-time method to get top N documents for a query.
 *
 *    ,-.
 *    `-'
 *    /|\
 *     |            ,--------------.          ,-------------.          ,-------------.          ,--------.          ,----------------.
 *    / \           |QueryProcessor|          |QueryExecutor|          |InvertedIndex|          |Document|          |SnippetGenerator|
 *   User           `------+-------'          `------+------'          `------+------'          `---+----'          `-------+--------'
 *    |        query       |                         |                        |                     |                       |
 *    | ------------------->                         |                        |                     |                       |
 *    |                    |                         |                        |                     |                       |
 *    |     ,-----------!. |                         |                        |                     |                       |
 *    |     |tokenize   |_\|----.                    |                        |                     |                       |
 *    |     |stopping     ||    | prepare            |                        |                     |                       |
 *    |     |stemming     ||<---'                    |                        |                     |                       |
 *    |     |create plan  ||                         |                        |                     |                       |
 *    |     `-------------'|                         |                        |                     |                       |
 *    |                    |       query plan        |                        |                     |                       |
 *    |                    | ----------------------->|                        |                     |                       |
 *    |                    |                         |                        |                     |                       |
 *    |                    |                         |                        |                     |                       |
 *    |                   ___________________________________________________________________________________________________
 *    |                   !|OPT  /  for each term    |                        |                     |                       |!
 *    |                   !_____/                    |                        |                     |                       |!
 *    |                   !|                         |         scan           |                     |                       |!
 *    |                   !|                         |----------------------->|                     |                       |!
 *    |                   !|                         |                        |                     |                       |!
 *    |                   !|                         |                        |                     |                       |!
 *    |                   !|        _______________________________________________________________________________         |!
 *    |                   !|        ! OPT  /  for each document               |                     |              !        |!
 *    |                   !|        !_____/          |                        |                     |              !        |!
 *    |                   !|        !                |                assign score                  |              !        |!
 *    |                   !|        !                |--------------------------------------------->|              !        |!
 *    |                   !|        !~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!        |!
 *    |                   !~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~!
 *    |                    |                         |                        |                     |                       |
 *    |                    |                         |               get top N docs                 |                       |
 *    |                    |                         |--------------------------------------------->|                       |
 *    |                    |                         |                        |                     |                       |
 *    |                    |                         |                 top N docs                   |                       |
 *    |                    |                         |<---------------------------------------------|                       |
 *    |                    |                         |                        |                     |                       |
 *    |                    |                         |                        | generate snippet    |                       |
 *    |                    |                         |--------------------------------------------------------------------->|
 *    |                    |                         |                        |                     |                       |
 *    |                    |                         |                        |     snippets        |                       |
 *    |                    |                         |<- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -|
 *    |                    |                         |                        |                     |                       |
 *    |                    | (url, title, snippet)*  |                        |                     |                       |
 *    |                    | <- - - - - - - - - - - -|                        |                     |                       |
 *    |                    |                         |                        |                     |                       |
 *    |                    |----.                    |                        |                     |                       |
 *    |                    |    | render results     |                        |                     |                       |
 *    |                    |<---'                    |                        |                     |                       |
 *    |                    |                         |                        |                     |                       |
 *    |       results      |                         |                        |                     |                       |
 *    | <-------------------                         |                        |                     |                       |
 *   User           ,------+-------.          ,------+------.          ,------+------.          ,---+----.          ,-------+--------.
 *    ,-.           |QueryProcessor|          |QueryExecutor|          |InvertedIndex|          |Document|          |SnippetGenerator|
 *    `-'           `--------------'          `-------------'          `-------------'          `--------'          `----------------'
 *    /|\
 *     |
 *    / \
 *
 *
 * This file has a list of Momdero API.
 */

trait SearchedDocument {
  /**
   * URL of the document.
   */
  val url : String

  /**
   * The title of the document.
   */
  val title : String

  /**
   * Document snippet with highlighting done using HTML.
   */
  val snippet : String
}

trait SearchResult {
  /**
   * Ranking of the first document in the documents array.
   */
  val firstRank : Int

  /**
   * The list of searched document. The ranking starts from firstRank at the documents(0) and
   * increases by one for each element in the array.
   */
  val documents : Array[SearchedDocument]
}

trait TMomdero {
  def search(query:String) : SearchResult
}

class MomderoImpl(path : String) extends TMomdero {
  override def search(query:String) : SearchResult = {
    // TODO : implement
    null
  }
}

object TMomdero {
  def open(path:String) : TMomdero = new MomderoImpl(path)
}


















