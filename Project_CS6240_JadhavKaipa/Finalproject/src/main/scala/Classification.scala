import java.io.File
import java.net.URI
import LabelledPreprocess.labelledPreprocess
import UnlabelledPreprocess.unlabelledPreprocess
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.hadoop.fs._
import org.apache.spark.mllib.tree.model.RandomForestModel

object Classification {

  /*
   * This is the main spark program used for training a model and using the trained model
   * for classifying unlabelled data. The program uses the library MLLib by Spark to train
   * a model using a Random Forest approach by using 70% of the input labelled data for
   * training and tests it on 30% of the remaining data, before outputting an error rate
   * for the model. It then uses the trained model and predicts the labels for unlabelled
   * input data, before storing the (identifier, label) data into a human readable CSV file.
   * The program takes 3 inputs, the folder location for labelled data, the folder location
   * for unlabelled data and the folder location to create final output. The output file
   * is named "JadhavKaipa.csv" in accordance to the requirements for the project and will
   * be stored in the output location specified as argument.
   *
   * Example courtesy: https://spark.apache.org/docs/latest/mllib-ensembles.html#random-forests
   */

  // merge function for creating one file
  // courtesy: http://www.markhneedham.com/blog/2014/11/30/spark-write-to-csv-file/
  def merge(spark: SparkContext, srcPath: String, dstPath: String): Unit =  {
    val srcSystem = FileSystem.get(new URI(srcPath), spark.hadoopConfiguration)
    val dstSystem = FileSystem.get(new URI(dstPath), spark.hadoopConfiguration)
    FileUtil.copyMerge(srcSystem, new Path(srcPath), dstSystem, new Path(dstPath), true, spark.hadoopConfiguration, null)
  }

  def main(args: Array[String]) {

    if (args.length != 3) {
      System.err.println("Usage: Classification <folder> <partitions> <task>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Classification")
    val spark = new SparkContext(conf)

    if (args(2).equals("training")) {
      /*
       * Pre-process the labelled data and store it in a location
       * for future use for training the model
       */

      val labelled = spark.textFile(args(0) + "/labelled").repartition(args(1).toInt)
      val labelledProcessed = labelled
        .map(line => labelledPreprocess(line))
        .filter(data => data != null)
      labelledProcessed.saveAsTextFile(args(0) + "/labelled_processed")

      /*
       * Train the model using Random Forest approach with number of trees and depth given below
       */

      // load the pre-processed labelled data and split it for training and testing
      val labelledData = MLUtils.loadLibSVMFile(spark, args(0) + "/labelled_processed").repartition(args(1).toInt)
      val splits = labelledData.randomSplit(Array(0.7, 0.3))
      val (trainingData, testData) = (splits(0), splits(1))

      val numClasses = 2
      val categoricalFeaturesInfo = Map[Int, Int]()
      val numTrees = 200
      val featureSubsetStrategy = "auto"
      val impurity = "gini"
      val maxDepth = 10
      val maxBins = 32

      // train the model
      val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
        numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

      // use the trained model to predict data for remainder of training data
      val labelledPrediction = testData.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }

      // Evaluate the accuracy of the trained model and output it to console
      val testErr = labelledPrediction.filter(r => r._1 != r._2).count.toDouble / testData.count()
      println("Test Error = " + testErr)

      // Save model for use during prediction
      model.save(spark, args(0) + "/model")

    } else if (args(2).equals("prediction")) {

      /*
       * Pre-process the unlabelled data and store it in a location
       * for future use for prediction using the model
       */

        val unlabelled = spark.textFile(args(0) + "/unlabelled")
        val unlabelledProcessed = unlabelled
          .map(line => unlabelledPreprocess(line))
          .filter(data => data != null)
        unlabelledProcessed.saveAsTextFile(args(0) + "/unlabelled_processed")

        /*
         * Load the pre-processed unlabelled data and for each data point, predict
         * the point's label and output it with the data point's unique identifier
         */

        val unlabelledData = MLUtils.loadLibSVMFile(spark, args(0) + "/unlabelled_processed")

        // load the model trained
        val model = RandomForestModel.load(spark, args(0) + "/model")

        // predist label and prepare data for saving in csv
        val unlabelledPrediction = unlabelledData.map { point =>
          val prediction = model.predict(point.features)
          ("S" + point.label.toInt.toString, prediction)
        }.map { case (key, value) => Array(key, value).mkString(",") }

        // prepare for storing predicted labels into csv file
        val file = args(0) + "/output/initial"
        FileUtil.fullyDelete(new File(file))
        val finalFile = args(0) + "/output/JadhavKaipa.csv"
        FileUtil.fullyDelete(new File(finalFile))

        // save the multipart label prediction data into a folder and use
        // merge function defined above to merge into single csv file
        unlabelledPrediction.saveAsTextFile(file)
        merge(spark, file, finalFile)

    }

    spark.stop()

  }

}
