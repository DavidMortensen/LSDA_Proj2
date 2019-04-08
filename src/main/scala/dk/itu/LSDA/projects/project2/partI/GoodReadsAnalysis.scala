package dk.itu.LSDA.projects.project2.partI

import com.typesafe.config.ConfigFactory
import dk.itu.LSDA.projects.project2.Timing
import org.apache.log4j.Level.INFO
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GoodReadsAnalysis {

  val log = LogManager.getRootLogger
  log.setLevel(INFO)

  val conf: SparkConf = new SparkConf().setAppName("GoodreadsBookReviewsAnalysis").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)


  /**
    * Load the data from a cvs file and parse it into and RDD of book reviews (RDD[BookReview])
    * @param filePath
    * @return RDD of BookReview objects
    */
  def dataLoader(filePath : String): RDD[BookReview] ={
    sc.textFile(filePath).map(data => BookReview(data))
  }


  /**
    * Find a list of distinct genres in all the BookReviews
    * @param bookReviews
    * @return list of distinct generes appearing in all book reviews
    */
  def findBookGenres(bookReviews : RDD[BookReview]):List[String] = ???


  //Approach 1:
  /**
    * For a given genre, find a count of all the BookReviews that describe a book by that genre
    * @param genre
    * @param bookReviews
    * @return a count
    */
  def genreBookCount(genre: String, bookReviews : RDD[BookReview]):Int = ???

  /**
    * generate a descendingly sorted list of pairs (genre, count of occurences)
    * @param bookReviewsRDD
    * @param bookGenres
    * @return pairs (genre, count of occurences)
    */
  def rankingByCounting(bookReviewsRDD: RDD[BookReview], bookGenres: List[String]): List[(String,Int)] = ???


  //Approach 2:
  /**
    * create an index where each genre points to all the BookReview that it is mentioned in them
    * @param bookReviewsRDD
    * @param bookGenres
    * @return an index that has the type: RDD[(String, Iterable[BookReview])]
    */
  def generateIndex(bookReviewsRDD: RDD[BookReview], bookGenres: List[String]) : RDD[(String, Iterable[BookReview])] = ???

  /**
    * Using an index of (genres, BookReviews), generate a descendingly sorted list of pairs (genre, count of occurences)
    * @param bookReviewsGenresIndex
    * @return pairs (genre, count of occurences)
    */
  def rankingUsingIndex(bookReviewsGenresIndex: RDD[(String, Iterable[BookReview])]): List[(String,Int)] = ???

  //Approach 3:
  /**
    * Given an RDD of book reviews and a list of genres occuring in all reviews, use reduceByKey to count the  BookReview that tags  book with a specific genre, for all genres
    * @param bookReviewsRDD
    * @param bookGenres
    * @return pairs (genre, count of occurences)
    */
  def rankingByReduction(bookReviewsRDD: RDD[BookReview], bookGenres: List[String]): List[(String,Int)] = ???


  def main(args: Array[String]): Unit = {
    //load file
    val filePath = ConfigFactory.load().getString("GoodreadsBookReviewsAnalysis.inputfilepath")
    val bookReviewsRDD = dataLoader(filePath)

    //find a list of distinct genres of the books
    val bookGenres = findBookGenres(bookReviewsRDD)

    //ranking  genres using counting technique
    println("First approach: Ranking by counting")
    val countsApproach1: List[(String, Int)] = Timing.time(rankingByCounting(bookReviewsRDD,bookGenres))


    //ranking genres using an index
    println("Second approach: Ranking by using an index")
    //create index
    val index = generateIndex(bookReviewsRDD,bookGenres)
    val countsApproach2: List[(String, Int)] = Timing.time(rankingUsingIndex(index))

    //ranking genres using reduction
    println("Third approach: Ranking by using ReduceByKey")
    val countsApproach3 = Timing.time(rankingByReduction(bookReviewsRDD,bookGenres))


    sc.stop()
  }

}