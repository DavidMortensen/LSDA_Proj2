package dk.itu.LSDA.projects.project2.partI

import com.typesafe.config.ConfigFactory
import dk.itu.LSDA.projects.project2.Timing
import org.apache.log4j.Level.INFO
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GoodReadsAnalysis {
  org.apache.log4j.Logger getLogger "org" setLevel (org.apache.log4j.Level.OFF)
  org.apache.log4j.Logger getLogger "akka" setLevel (org.apache.log4j.Level.OFF)

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
  def findBookGenres(bookReviews : RDD[BookReview]):List[String] = {
  bookReviews.flatMap(review => review.genres).distinct().collect().toList
  }


  //Approach 1:
  /**
    * For a given genre, find a count of all the BookReviews that describe a book by that genre
    * @param genre
    * @param bookReviews
    * @return a count
    */
    
  def genreBookCount(genre: String, bookReviews : RDD[BookReview]):Int = {
  bookReviews.flatMap(review => review.genres).filter(_.equals(genre)).count().toInt
  }


  /**
    * generate a descendingly sorted list of pairs (genre, count of occurences)
    * @param bookReviewsRDD
    * @param bookGenres
    * @return pairs (genre, count of occurences)
    */
  def rankingByCounting(bookReviewsRDD: RDD[BookReview], bookGenres: List[String]): RDD[(String,Int)] = {
  //println(bookGenres.map(s => (s, genreBookCount(s, bookReviewsRDD))).sortBy(_._2).reverse)
  sc.parallelize(bookGenres.map(s => (s, genreBookCount(s, bookReviewsRDD))).sortBy(_._2).reverse)


  }

  //Approach 2:
  /**
    * create an index where each genre points to all the BookReview that it is mentioned in them
    * @param bookReviewsRDD
    * @param bookGenres
    * @return an index that has the type: RDD[(String, Iterable[BookReview])]  : RDD[(String, Iterable[BookReview])]
    */
  def generateIndex(bookReviewsRDD: RDD[BookReview], bookGenres: List[String]) : RDD[(String, Iterable[BookReview])]  = {
  println()
  println()

  //val tuple = bookGenres.map(genre => (genre, bookReviewsRDD.filter(review => review.genres.contains(genre)).collect().toList.toIterable))
  //val rdd = sc.parallelize(tuple)
  //rdd

  val tuple = bookReviewsRDD.flatMap(review => review.genres.map(genre => (genre, review))).groupByKey()

  //println(tuple)
  tuple

  }
  
  /**
    * Using an index of (genres, BookReviews), generate a descendingly sorted list of pairs (genre, count of occurences)
    * @param bookReviewsGenresIndex
    * @return pairs (genre, count of occurences)
    */
  def rankingUsingIndex(bookReviewsGenresIndex: RDD[(String, Iterable[BookReview])]): RDD[(String,Int)] = {
  println()
  println()


  val usingIndex = bookReviewsGenresIndex.map(genre =>(genre._1, genre._2.size))
  val sorted = usingIndex.sortBy(-_._2)
  sorted

  }


  //Approach 3:
  /**
    * Given an RDD of book reviews and a list of genres occuring in all reviews, use reduceByKey to count the  BookReview that tags  book with a specific genre, for all genres
    * @param bookReviewsRDD
    * @param bookGenres
    * @return pairs (genre, count of occurences)
    */

  def rankingByReduction(bookReviewsRDD: RDD[BookReview], bookGenres: List[String]): RDD[(String,Int)] = {
  //val reduct = bookReviewsRDD.collect()
  

  val tuple = bookReviewsRDD.flatMap(review => review.genres.map(genre => (genre, 1))).reduceByKey(_ + _)
  tuple
  }

  

  def main(args: Array[String]): Unit = {
    //load file
    val filePath = ConfigFactory.load().getString("GoodreadsBookReviewsAnalysis.inputfilepath")
    val bookReviewsRDD = dataLoader(filePath)

    //find a list of distinct genres of the books
    val bookGenres = findBookGenres(bookReviewsRDD.persist())
  
    //println(bookGenres)


    //ranking  genres using counting technique
    println("First approach: Ranking by counting")
    val countsApproach1: RDD[(String, Int)] = Timing.time(rankingByCounting(bookReviewsRDD,bookGenres))


    //ranking genres using an index
    println("Second approach: Ranking by using an index")
    //create index
    val index = generateIndex(bookReviewsRDD,bookGenres)
    val countsApproach2: RDD[(String, Int)] = Timing.time(rankingUsingIndex(index))

    //ranking genres using reduction
    println()
    println()
    println()
    println()

    println("Third approach: Ranking by using ReduceByKey")
    val countsApproach3 = Timing.time(rankingByReduction(bookReviewsRDD,bookGenres))


    sc.stop()
  }

}

