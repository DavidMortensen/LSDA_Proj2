package dk.itu.LSDA.projects.project2.partII

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object YelpAnalysis {

  val spark = SparkSession.builder().appName("YelpReviewsAnalysis").master("local").getOrCreate()


  def dataLoader(path: String):DataFrame ={
    spark.read.json(path)
  }

  //Q1:
  /**
    *  use SQL statements: add all the number of reviews for all businesses
    * @param yelpBusinesses
    * @return a dataframe with one value representing the total number of reviews for all businesses
    */
  def totalReviewsSQL(yelpBusinesses : DataFrame):DataFrame = {
      yelpBusinesses.createTempView("YelpBus")
      spark.sql("SELECT sum(review_count) FROM YelpBus")
}
  /**
    * use DataFrame transformations: add all the number of reviews for all businesses
    * @param yelpBusinesses
    * @return a dataframe with one value representing the total number of reviews for all businesses
    */
  def totalReviewsbDF(yelpBusinesses : DataFrame):DataFrame = {
    yelpBusinesses.select(sum("review_count"))
  }
  
  //Q2:
  /**
    * use SQL statements: find all businesses that have received 5 stars and that have been reviewed by 1000 or more users
    * @param yelpBusinesses
    * @return a Dataframe of (name, stars, review_count) of five star businesses
    */
  def fiveStarBusinessesSQL(yelpBusinesses: DataFrame):DataFrame = {
    //yelpBusinesses.createTempView("YelpBus")
    spark.sql("SELECT name, stars, review_count FROM YelpBus WHERE review_count > 1000 and stars = 5")
  }

  /**
    * use DataFrame transformations: find all businesses that have received 5 stars and that have been reviewed by 1000 or more users
    * @param yelpBusinesses
    * @return a Dataframe of (name, stars, review_count) of five star businesses
    */
  def fiveStarBusinessesDF(yelpBusinesses: DataFrame):DataFrame = {
    yelpBusinesses.select("name" , "stars", "review_count").filter("review_count > 1000 and stars = 5")
  }

  //Q3:
  /**
    * use SQL statements: find the influencer users who have written more than 1000 reviews
    * @param yelpUsers
    * @return DataFrame of user_id of influencer users
    */
  def findInfluencerUserSQL(yelpUsers : DataFrame):DataFrame = {
    yelpUsers.createTempView("YelpUsers")
    spark.sql("SELECT user_id FROM YelpUsers WHERE review_count > 1000")
  }

  /**
    * use DataFrame transformations: find the influencer users who have written more than 1000 reviews
    * @param yelpUsers
    * @return DataFrame of user_id of influencer users
    */
  def findInfluencerUserDF(yelpUsers : DataFrame):DataFrame = {
    yelpUsers.select("user_id").filter("review_count > 1000")
  }

  //Q4:
  /**
    * use SQL statements: find the businesses that have been reviewed by more than 5 influencer users
    * @param yelpBusinesses
    * @param yelpReviews
    * @param influencerUsers
    * @return DataFrame of names of businesses that match the criteria
    */
  def findFamousBusinessesSQL(yelpBusinesses: DataFrame, yelpReviews: DataFrame, influencerUsers: DataFrame) : DataFrame = {
    spark.sql("SELECT name FROM YelpBus WHERE review_count > 5")
    }

  /**
    * use DataFrame transformations: find the businesses that have been reviewed by more than 5 influencer users
    * @param yelpBusinesses
    * @param yelpReviews
    * @param influencerUsersDF
    * @return DataFrame of names of businesses that match the criteria
    */
  def findFamousBusinessesDF(yelpBusinesses: DataFrame, yelpReviews: DataFrame, influencerUsersDF: DataFrame): DataFrame = ???

  //Q5:
  /**
    * use SQL statements: find a descendingly ordered list of users based on their the average star counts given by each of them
    * in all the reviews that they have written
    *
    * You need to average the stars given by each user in reviews that appear in yelpReviews and then sort them
    *
    * @param yelpReviews
    * @param yelpUsers
    * @return DataFrame of (user names and average stars)
    */
  def findavgStarsByUserSQL(yelpReviews: DataFrame, yelpUsers: DataFrame):DataFrame = ???

  /**
    * use DataFrame transformations: find a descendingly ordered list of users based on their the average star counts given by each of them
    * in all the reviews that they have written
    *
    * You need to average the stars given by each user in reviews that appear in yelpReviews and then sort them
    *
    * @param yelpReviews
    * @param yelpUsers
    * @return DataFrame of (user names and average stars)
    */
  def findavgStarsByUserDF(yelpReviews: DataFrame, yelpUsers: DataFrame):DataFrame = ???


  def main(args: Array[String]): Unit = {
    import spark.implicits._

    //load yelp data
    val yelpReviewsFilePath = ConfigFactory.load().getString("YelpData.yelpReviewsFilePath")
    val yelpReviews = dataLoader(yelpReviewsFilePath)
    //println("Schema for reviews: "+ yelpReviews.schema)

    val yelpBusinessFilePath = ConfigFactory.load().getString("YelpData.yelpBusinessFilePath")
    val yelpBusiness = dataLoader(yelpBusinessFilePath)
    //println("Schema for business: "+ yelpBusiness.schema)
    //yelpBusiness.show()


    val yelpUserFilePath = ConfigFactory.load().getString("YelpData.yelpUserFilePath")
    val yelpUsers = dataLoader(yelpUserFilePath)
    //println("Schema for users: "+ yelpUsers.schema)



    // Q1: Analyze yelp_academic_dataset_Business.json to find the number of reviews for all businesses.
    // The output should be in the form of DataFrame of a single count.
    println("Q1: query yelp_academic_dataset_Business.json to find the total number of reviews for all businesses")
    val totalReviewsPerBusinessSQL = totalReviewsSQL(yelpBusiness)

    println("total number of reviews - SQL query: "+ totalReviewsPerBusinessSQL.first().getLong(0))
    val totalReviewsPerBusinessDF = totalReviewsbDF(yelpBusiness)

    //println("total number of reviews - DF Analysis: "+ totalReviewsPerBusinessDF.first().getLong(0))


    // Q2:  Analyze Analyze yelp_academic_dataset_Business.json to find all businesses that have received 5 stars and that have been reviewed by 1000 or more users
    println("Q2: query yelp_academic_dataset_Business.json to find businesses that have received 5 stars and that have been reviewed by 1000 or more users")
    val topBusinessesSQL = fiveStarBusinessesSQL(yelpBusiness)
    topBusinessesSQL.show()

    val topBusinessesDF = fiveStarBusinessesDF(yelpBusiness)
    topBusinessesDF.show()

    // Q3:Analyze yelp_academic_dataset_users.json to find the influencer users who have written more than 1000 reviews.
    println("Q3: query yelp_academic_dataset_users.json to find influencers")
    val influencerUsersSQL = findInfluencerUserSQL(yelpUsers)
    influencerUsersSQL.show()

    val influencerUsersDF = findInfluencerUserDF(yelpUsers)
    influencerUsersSQL.show()

    // Q4: Analyze yelp\_academic_dataset_review.json and a view created from your answer to Q3  to find names of businesses that have been reviewed by more than 5 influencer users.
    println("Q3: query yelp_academic_dataset_reviews.json  to find businesses reviewd by more than 5 influencers")
    val businessesReviewedByInfluencersDF = findFamousBusinessesDF(yelpBusiness, yelpReviews, influencerUsersDF)

    // Q5: Analyze yelp_academic_dataset_review.json  and yelp_academic_dataset_users.json to find the average stars given by each user. You need to order the users according to their average star counts.
    //println("Q5: query yelp_academic_dataset_reviews.json, query yelp_academic_dataset_users.json to find average stars given by each user, descendingly ordered")
    //al avgStarsByUserSQL = findavgStarsByUserSQL(yelpReviews,yelpUsers)

    //val avgStarsByUserDF = findavgStarsByUserDF(yelpReviews, yelpUsers)


    spark.close()
  }
}
