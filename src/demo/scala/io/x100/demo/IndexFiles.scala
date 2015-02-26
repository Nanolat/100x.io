package io.x100.demo

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import io.x100.api._

import java.io.BufferedReader
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes
import java.util.Date


/** Index all text files under a directory.
  * <p>
  * This is a command-line application demonstrating simple 100x.io indexing.
  * Run it with no command-line arguments for usage information.
  */
object IndexFiles {
  /** Index all text files under a directory. */
  def main(args : Array[String]) {
    val usage = "java org.apache.lucene.demo.IndexFiles" +
      " [-index INDEX_PATH] [-docs DOCS_PATH] [-update]\n\n" +
      "This indexes the documents in DOCS_PATH, creating a Lucene index" +
      "in INDEX_PATH that can be searched with SearchFiles"
    var indexPath = "index"
    var docsPath : String = null
    var create = true
    var i = 0
    while(i < args.length) {
      if ("-index".equals(args(i))) {
        indexPath = args(i+1)
        i += 1
      } else if ("-docs".equals(args(i))) {
        docsPath = args(i+1)
        i += 1
      } else if ("-update".equals(args(i))) {
        create = false;
      }
      i += 1
    }

    if (docsPath == null) {
      System.err.println("Usage: " + usage)
      System.exit(1)
    }

    val docDir : Path = Paths.get(docsPath);
    if (!Files.isReadable(docDir)) {
      System.out.println("Document directory '" +docDir.toAbsolutePath()+ "' does not exist or is not readable, please check the path");
      System.exit(1);
    }

    val start = new Date();
    try {
      System.out.println("Indexing to directory '" + indexPath + "'...");

      val dir : Directory = Directory.open("filesystem", Paths.get(indexPath))
      val analyzer : Analyzer = Analyzer.create("standard");
      val iwc : IndexWriterConfig = IndexWriterConfig.create(analyzer);

      if (create) {
        // Create a new index in the directory, removing any
        // previously indexed documents:
        iwc.setOpenMode(IndexWriterOpenMode.CREATE);
      } else {
        // Add new documents to an existing index:
        iwc.setOpenMode(IndexWriterOpenMode.CREATE_OR_APPEND);
      }

      // Optional: for better indexing performance, if you
      // are indexing many documents, increase the RAM
      // buffer.  But if you do this, increase the max heap
      // size to the JVM (eg add -Xmx512m or -Xmx1g):
      //
      // iwc.setRAMBufferSizeMB(256.0);

      val writer : IndexWriter = IndexWriter.create(dir, iwc);
      indexDocs(writer, docDir);

      // NOTE: if you want to maximize search performance,
      // you can optionally call forceMerge here.  This can be
      // a terribly costly operation, so generally it's only
      // worth it when your index is relatively static (ie
      // you're done adding documents to it):
      //
      // writer.forceMerge(1);

      writer.close();

      val end = new Date();
      System.out.println(end.getTime() - start.getTime() + " total milliseconds");

    } catch {
      case e : IOException => {
        System.out.println(" caught a " + e.getClass() +
          "\n with message: " + e.getMessage());
      }
    }
  }

  /**
   * Indexes the given file using the given writer, or if a directory is given,
   * recurses over files and directories found under the given directory.
   *
   * NOTE: This method indexes one document per input file.  This is slow.  For good
   * throughput, put multiple documents into your input file(s).  An example of this is
   * in the benchmark module, which can create "line doc" files, one document per line,
   * using the
   * <a href="../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
   * >WriteLineDocTask</a>.
   *
   * @param writer Writer to the index where the given file/dir info will be stored
   * @param path The file to index, or the directory to recurse into to find files to index
   * @throws IOException If there is a low-level I/O error
   */
  def indexDocs(writer : IndexWriter, path : Path) {
    if (Files.isDirectory(path)) {
      Files.walkFileTree(path, new SimpleFileVisitor[Path]() {
        override def visitFile(file : Path, attrs : BasicFileAttributes) : FileVisitResult = {
          try {
            indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
          } catch {
            case e:IOException => {
              // Do nothing.
            }
            // don't index files that can't be read.
          }
          return FileVisitResult.CONTINUE;
        }
      });
    } else {
      indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
    }
  }

  /** Indexes a single document */
  def indexDoc(writer : IndexWriter, file : Path, lastModified : Long) : Unit = {
    var stream : InputStream= null

    try {
      stream = Files.newInputStream(file)
      // make a new, empty document
      val doc = Document.create()

      // Add the path of the file as a field named "path".  Use a
      // field that is indexed (i.e. searchable), but don't tokenize
      // the field into separate words and don't index term frequency
      // or positional information:
      val pathField : Field = new StringField("path", file.toString() );
      doc.addField(pathField);

      // Add the last modified date of the file a field named "modified".
      // Use a LongField that is indexed (i.e. efficiently filterable with
      // NumericRangeFilter).  This indexes to milli-second resolution, which
      // is often too fine.  You could instead create a number based on
      // year/month/day/hour/minutes/seconds, down the resolution you require.
      // For example the long value 2011021714 would mean
      // February 17, 2011, 2-3 PM.
      doc.addField(new LongField("modified", lastModified));

      // Add the contents of the file to a field named "contents".  Specify a Reader,
      // so that the text of the file is tokenized and indexed, but not stored.
      // Note that FileReader expects the file to be in UTF-8 encoding.
      // If that's not the case searching for special characters will fail.
      doc.addField(new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

      if (writer.getConfig().getOpenMode() == IndexWriterOpenMode.CREATE) {
        // New index, so we just add the document (no old document can be there):
        System.out.println("adding " + file);
        writer.addDocument(doc);
      } else {
        // Existing index (an old copy of this document may have been indexed) so
        // we use updateDocument instead to replace the old one matching the exact
        // path, if present:
        System.out.println("updating " + file);
        writer.updateDocument(new Term("path", file.toString()), doc);
      }
    } finally {
      if (stream != null)
        stream.close()
    }
  }
}
