package io.x100.demo;

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
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import java.util.Date

import scala.util.control.Breaks._

/** Simple command-line based search demo. */
object SearchFiles {

  /** Simple command-line based search demo. */
  def main(args : Array[String]) {
    val usage =
      "Usage:\tjava org.apache.lucene.demo.SearchFiles [-index dir] [-field f] [-repeat n] [-queries file] [-query string] [-raw] [-paging hitsPerPage]\n\nSee http://lucene.apache.org/core/4_1_0/demo/ for details.";
    if (args.length > 0 && ("-h".equals(args(0)) || "-help".equals(args(0)))) {
      System.out.println(usage)
      System.exit(0)
    }

    var index = "index"
    var field = "contents"
    var queries : String = null
    var repeat = 0
    var raw = false
    var queryString : String = null
    var hitsPerPage = 10;

    var i = 0
    while(i < args.length) {
      if ("-index".equals(args(i))) {
        index = args(i+1);
        i += 1
      } else if ("-field".equals(args(i))) {
        field = args(i+1);
        i += 1
      } else if ("-queries".equals(args(i))) {
        queries = args(i+1);
        i += 1
      } else if ("-query".equals(args(i))) {
        queryString = args(i+1);
        i += 1
      } else if ("-repeat".equals(args(i))) {
        repeat = Integer.parseInt(args(i+1));
        i += 1
      } else if ("-raw".equals(args(i))) {
        raw = true;
      } else if ("-paging".equals(args(i))) {
        hitsPerPage = Integer.parseInt(args(i+1));
        if (hitsPerPage <= 0) {
          System.err.println("There must be at least 1 hit per page.");
          System.exit(1);
        }
        i += 1
      }
      i += 1
    }

    val reader : IndexReader = DirectoryReader.open(Directory.open(DirectoryType.FILE_SYSTEM, Paths.get(index)));
    val searcher : IndexSearcher = IndexSearcher.create(reader);
    val analyzer : Analyzer = Analyzer.create(AnalyzerType.STANDARD);

    val in = if (queries != null) {
      Files.newBufferedReader(Paths.get(queries), StandardCharsets.UTF_8);
    } else {
      new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    }

    val parser : QueryParser = QueryParser.create(field, analyzer);
    while (true) {
      if (queries == null && queryString == null) {                        // prompt the user
        System.out.println("Enter query: ");
      }

      var line = if (queryString != null) queryString else in.readLine();

      if (line == null || line.length() == -1) {
        break;
      }

      line = line.trim();
      if (line.length() == 0) {
        break;
      }

      val query : Query = parser.parse(line);
      System.out.println("Searching for: " + query.toString(field));

      if (repeat > 0) {                           // repeat & time as benchmark
        val start = new Date();
        for (i <- 1 to 100) {
          searcher.search(query, 100);
        }
        val end = new Date();
        System.out.println("Time: "+(end.getTime()-start.getTime())+"ms");
      }

      doPagingSearch(in, searcher, query, hitsPerPage, raw, queries == null && queryString == null);

      if (queryString != null) {
        break;
      }
    }
    reader.close();
  }

  /**
   * This demonstrates a typical paging search scenario, where the search engine presents 
   * pages of size n to the user. The user can then go to the next page if interested in
   * the next hits.
   *
   * When the query is executed for the first time, then only enough results are collected
   * to fill 5 result pages. If the user wants to page beyond this limit, then the query
   * is executed another time and all hits are collected.
   *
   */
  def doPagingSearch(in : BufferedReader, searcher : IndexSearcher, query : Query,
                     hitsPerPage : Int, raw : Boolean, interactive : Boolean) {

    // Collect enough docs to show 5 pages
    val results : TopDocuments = searcher.search(query, 5 * hitsPerPage);
    var hits : Array[ScoreDocument] = results.scoreDocuments();

    val numTotalHits : Int = results.totalHits;
    System.out.println(numTotalHits + " total matching documents");

    var start = 0;
    var end = Math.min(numTotalHits, hitsPerPage);

    while (true) {
      if (end > hits.length) {
        System.out.println("Only results 1 - " + hits.length +" of " + numTotalHits + " total matching documents collected.");
        System.out.println("Collect more (y/n) ?");
        val line = in.readLine();
        if (line.length() == 0 || line.charAt(0) == 'n') {
          break;
        }

        hits = searcher.search(query, numTotalHits).scoreDocuments;
      }

      end = Math.min(hits.length, start + hitsPerPage);

      for (i <- start until end) {
        if (raw) {                              // output raw format
          System.out.println("documentId="+hits(i).documentId+" score="+hits(i).score);
        } else {
          val doc : StoredDocument = searcher.document(hits(i).documentId);
          val path : String = doc.getFieldValue("path");
          if (path != null) {
            System.out.println((i+1) + ". " + path);
            val title = doc.getFieldValue("title");
            if (title != null) {
              System.out.println("   Title: " + doc.getFieldValue("title"));
            }
          } else {
            System.out.println((i+1) + ". " + "No path for this document");
          }
        }
      }

      if (!interactive || end == 0) {
        break;
      }

      if (numTotalHits >= end) {
        var quit = false;
        while (true) {
          System.out.print("Press ");
          if (start - hitsPerPage >= 0) {
            System.out.print("(p)revious page, ");
          }
          if (start + hitsPerPage < numTotalHits) {
            System.out.print("(n)ext page, ");
          }
          System.out.println("(q)uit or enter number to jump to a page.");

          val line = in.readLine();
          if (line.length() == 0 || line.charAt(0)=='q') {
            quit = true;
            break;
          }
          if (line.charAt(0) == 'p') {
            start = Math.max(0, start - hitsPerPage);
            break;
          } else if (line.charAt(0) == 'n') {
            if (start + hitsPerPage < numTotalHits) {
              start+=hitsPerPage;
            }
            break;
          } else {
            val page = Integer.parseInt(line);
            if ((page - 1) * hitsPerPage < numTotalHits) {
              start = (page - 1) * hitsPerPage;
              break;
            } else {
              System.out.println("No such page");
            }
          }
        }
        if (quit) break;
        end = Math.min(numTotalHits, start + hitsPerPage);
      }
    }
  }
}
