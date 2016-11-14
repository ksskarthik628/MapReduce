import Bz2WikiParser.parser
import org.apache.spark.{SparkConf, SparkContext}

/*
 * This is the main class (a singleton object) that acts as the driver for Page Rank program.
 * This class's main function takes either two or three inputs, first being the input folder location,
 * second being the output folder location and the third optional input being the number of iterations
 * of Page Rank to execute. By default, a value of 10 is given to the program when there is no third
 * argument. After parsing the input data, the program runs through the number of iterations specified
 * (or 10 by default) of the Page Rank algorithm. After running the Page Rank algorithm, the final
 * output is the list of top k (100 here) pages in the given list of pages.
 * 
 * Idea inspiration : https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkPageRank.scala
 */

object SparkPageRank {
  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: SparkPageRank <input> <output> [optional: <iterations>]")
      System.exit(1)
    }

    // Global variables to make use during the program execution
    val randomness = 0.85
    val k = 100
    val machines = 1
    val sortOrder = -1

    // Create a Spark Configuration like in Hadoop and a job context to run the program
    val conf = new SparkConf().setAppName("SparkPageRank")
    val spark = new SparkContext(conf)

    // Take number of iterations from arguments if available, else assign default as 10
    val iters = if (args.length > 2) args(2).toInt else 10

    // Read input from folder given in arguments
    val lines = spark.textFile(args(0))

    /*
     * For each line in the input taken, send the line to the parser. The value returned from the parser can be a string
     * or a null. If it is a null, emit null. If it is not a null, split it into page name and adjacency list, remove
     * the leading and ending bracket artifacts from the parser and emit the pair (pageName, adjacencyList). Since
     * we don't need null values in our final graph, filter out the null values and persist this graph in each machine.
     */
    val links = lines
      .map(line => parser(line))
      .map { line =>
        if (line != null) {
          val parts = line.split(":")
          val len = parts(1).length - 1
          (parts(0).trim, parts(1).substring(1, len))
        } else null
      }
      .filter(data => data != null)
      .persist()

    // Take the total count of valid pages in the graph
    val total = links.count()
    // Create an initial ranking of 1/#pages for each page in the graph
    var ranks = links.mapValues(v => 1.0 / total)
    // Initialize the dangling node accumulator for storing sum of dangling node contribution to Page Rank
    val dangling = spark.doubleAccumulator("Dangling Node")

    // Perform the Page Rank as many times given in arguments (or 10 by default)
    for (i <- 1 to iters) {
      // reset te dangling node contribution for the next iteration
      dangling.reset()
      /*
       * Join the graph RDD with ranks RDD to create an RDD with key and values (pageName, (adjacencyList, rank)). On
       * each such entry in the RDD, check whether the page is a dangling node. If it is not, for every url in it's
       * adjacency list, emit (adjacentPageName, rank/#adjacent). If it is a dangling node, add it's rank to the
       * dangling node accumulator and emit null. Filter out any null values after this operation is done. Add up all
       * duplicate entries in the so created RDD and convert this accumulated rank into the next page rank by using
       * the dangling node contribution as well.
       */
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val branches = urls.split(",")
        val size = branches.length
        if (branches(0) != "")
          branches.map(url => (url.trim, rank / size))
        else {
          dangling.add(rank)
          None
        }
      }.filter(data => data != null)
        .reduceByKey((a, b) => a + b)
        .mapValues(rank => ((1 - randomness) / total) + (randomness * rank) + ((randomness * dangling.value) / total))

      /*
       * The above created the new page rank only and only for those pages which are not dangling nodes. In order
       * to bring back the dangling nodes, take only those pages that were in the initial 'ranks' and not in the
       * 'contribs', convert their page ranks to the right value using dangling node contribution. This creates two
       * disjoint RDDs, one for pages that have in-links and one for dangling nodes. Do a union of the two to get
       * the new page rank values for all pages in the graph, dangling or not.
       */
      ranks = ranks.subtractByKey(contribs)
        .mapValues(v => ((1 - randomness) / total) + ((randomness * dangling.value) / total))
        .union(contribs)
    }

    /*
     * After all the page rank iterations have been run, sort the data so formed using the Page Rank for comparison
     * in decreasing order and take the top k pages based on their page ranks. Save the so formed data in the output
     * directory as mentioned in the arguments.
     */
    val topk = ranks.sortBy(value => sortOrder * value._2).take(k)
    spark.parallelize(topk, machines).saveAsTextFile(args(1))

    // Finish the spark job
    spark.stop()
  }
}
