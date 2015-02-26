package io.x100.api

import java.io._
;
import java.nio.file.Path;

private final class StandardAnalyzer extends Analyzer {
}

final object AnalyzerType extends Enumeration {
  type AnalyzerType = Value
  val STANDARD = Value
}
import AnalyzerType._

object Analyzer {
  def create(analyzerType : AnalyzerType) : Analyzer = {
    analyzerType match  {
      case STANDARD => new StandardAnalyzer()
    }
  }
}
trait Analyzer {
}

trait Field {
  val name : String
  val value : Any = null
  val stream : Reader = null
}

private trait TokenStream {
}

final class IntField(override val name : String, override val value : Int) extends Field
final class LongField(override val name : String, override val value : Long) extends Field
final class FloatField(override val name : String, override val value : Float) extends Field
final class DoubleField(override val name : String, override val value : Double) extends Field
final class StringField(override val name : String, override val value : String) extends Field
final class TextField(override val name : String, override val stream : Reader) extends Field

final object Document {
  def create() : Document = {
    assert(false) // TODO : Implement
    null
  }
}

trait Document {
  def addField(field : Field) : Unit
  def removeField(fieldName : String) : Unit
  def removeFields(fieldName : String) : Unit
  def getField(fieldName : String) : Field
  def getFields(fieldName : String) : Seq[Field]
  def getFields() : Seq[Field]
}

final object IndexWriter {
  def create(dir : Directory, config : IndexWriterConfig) : IndexWriter = {
    assert(false) // TODO : Implement
    null
  }
}

final object IndexWriterOpenMode extends Enumeration {
  type IndexWriterOpenMode = Value
  val CREATE, CREATE_OR_APPEND = Value
}

import IndexWriterOpenMode._

trait IndexWriter {
  def close() : Unit
  def getConfig() : IndexWriterConfig
  def addDocument(doc : Document) : Unit
  def updateDocument(term : Term, doc : Document) : Unit
}

final object IndexWriterConfig {
  def create(analyzer : Analyzer) : IndexWriterConfig = {
    assert(false) // TODO : Implement
    null
  }
}

trait IndexWriterConfig {
  def setOpenMode(openMode : IndexWriterOpenMode) : Unit
  def getOpenMode() : IndexWriterOpenMode
}

private final class FileSystemDirectory(path : Path) extends Directory {

}

final object DirectoryType extends Enumeration {
  type DirectoryType = Value
  val FILE_SYSTEM = Value
}

import DirectoryType._

final object Directory {
  def open(directoryType : DirectoryType, path : Path) : Directory = {
    directoryType match {
      case FILE_SYSTEM => new FileSystemDirectory(path)
    }
  }
}

trait Directory {
}


final object DirectoryReader {
  def open(dir : Directory) : IndexReader = {
    assert(false) // TODO : Implement
    null
  }
}

trait IndexReader {
  def document(documentId : Int) : StoredDocument
  def close() : Unit
}

trait StoredDocument {
  def getFieldValue(fieldName : String) : String
}

final object QueryParser {
  def create(defaultFieldName : String, analyzer : Analyzer) : QueryParser = {
    assert(false) // TODO : Implement
    null
  }
}

trait QueryParser {
  def parse(query:String) : Query
}

trait Query {
}

private final case class Term(fieldName : String, text : String)
private final case class TermQuery(term : Term) extends Query
/*
TODO : These are not supported yet. Support them in future versions.
private final case class BooleanQuery() extends Query
private final case class WildcardQuery() extends Query
private final case class PhraseQuery() extends Query
private final case class ProximityQuery() extends Query
*/

final object IndexSearcher {
  def create(indexReader : IndexReader ) : IndexSearcher = {
    new IndexSearcherImpl(indexReader)
  }
}

trait TopDocuments {
  def scoreDocuments() : Array[ScoreDocument]
  val totalHits : Int
}

private final class IndexSearcherImpl(val indexReader : IndexReader) extends IndexSearcher {
  def search(query : Query, count : Int) : TopDocuments = {
    assert(false) // TODO : Implement
    null
  }
  def document(documentId : Int) : StoredDocument = indexReader.document(documentId)
}
trait IndexSearcher {
  def search(query : Query, count : Int) : TopDocuments
  def document(documentId : Int) : StoredDocument
}

final case class ScoreDocument(documentId:Int, score : Int)
