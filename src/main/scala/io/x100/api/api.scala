package io.x100.api

import java.io._
;
import java.nio.file.Path;

class StandardAnalyzer extends Analyzer {
}

object AnalyzerType extends Enumeration {
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
}

class LongField(name : String, value : Long) extends Field
class StringField(name : String, value : String) extends Field
class TextField(name : String, reader : Reader) extends Field

object Document {
  def create() : Document = {
    assert(false) // TODO : Implement
    null
  }
}

trait Document {
  def add(field : Field) : Unit
}

object IndexWriter {
  def create(dir : Directory, config : IndexWriterConfig) : IndexWriter = {
    assert(false) // TODO : Implement
    null
  }
}

object IndexWriterOpenMode extends Enumeration {
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

object IndexWriterConfig {
  def create(analyzer : Analyzer) : IndexWriterConfig = {
    assert(false) // TODO : Implement
    null
  }
}

trait IndexWriterConfig {
  def setOpenMode(openMode : IndexWriterOpenMode) : Unit
  def getOpenMode() : IndexWriterOpenMode
}

case class Term(field : String, text : String)

class FileSystemDirectory(path : Path) extends Directory {

}

object DirectoryType extends Enumeration {
  type DirectoryType = Value
  val FILE_SYSTEM = Value
}

import DirectoryType._

object Directory {
  def open(directoryType : DirectoryType, path : Path) : Directory = {
    directoryType match {
      case FILE_SYSTEM => new FileSystemDirectory(path)
    }
  }
}

trait Directory {
}


object DirectoryReader {
  def open(dir : Directory) : IndexReader = {
    assert(false) // TODO : Implement
    null
  }
}

trait IndexReader {
  def close() : Unit
}

trait StoredDocument {
  def get(fieldName : String) : String
}

object QueryParser {
  def create(defaultFieldName : String, analyzer : Analyzer) : QueryParser = {
    assert(false) // TODO : Implement
    null
  }
}

trait QueryParser {
  def parse(query:String) : Query
}

trait Query {
  def toString(defaultFieldName : String) : String
}

object IndexSearcher {
  def create(indexReader : IndexReader ) : IndexSearcher = {
    assert(false) // TODO : Implement
    null
  }
}

trait TopDocuments {
  def scoreDocuments() : Array[ScoreDocument]
  val totalHits : Int
}

trait IndexSearcher {
  def search(query : Query, count : Int) : TopDocuments
  def document(documentId : Int) : StoredDocument
}

case class ScoreDocument(documentId:Int, score : Int)
