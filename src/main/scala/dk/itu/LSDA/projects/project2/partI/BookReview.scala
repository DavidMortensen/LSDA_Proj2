package dk.itu.LSDA.projects.project2.partI

case class BookReview (
                        book_authors: List[String],
                        book_desc : String,
                        book_edition: String,
                        book_format: String,
                        book_isbn:String,
                        book_pages: Int,
                        book_rating: Double,
                        book_rating_count: Int,
                        book_review_count: Int,
                        book_title:String,
                        genres:List[String],
                        image_url:String
                      )

object BookReview{
  def apply(bookReviewTxt : String):BookReview = {
    val bookReviewTxtSplitted = bookReviewTxt.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
    if(bookReviewTxtSplitted.length<12)
      println(bookReviewTxtSplitted)
    BookReview(if(bookReviewTxtSplitted(0).isEmpty) List[String]() else bookReviewTxtSplitted(0).split('|').toList,
      bookReviewTxtSplitted(1),
      bookReviewTxtSplitted(2),
      bookReviewTxtSplitted(3),
      bookReviewTxtSplitted(4),
      if(bookReviewTxtSplitted(5).isEmpty) 0 else bookReviewTxtSplitted(5).split(" ")(0).trim.toInt,
      if(bookReviewTxtSplitted(6).isEmpty) 0 else bookReviewTxtSplitted(6).trim.toDouble,
      if(bookReviewTxtSplitted(7).isEmpty) 0 else bookReviewTxtSplitted(7).trim.toInt,
      if(bookReviewTxtSplitted(8).isEmpty) 0 else bookReviewTxtSplitted(8).trim.toInt,
      bookReviewTxtSplitted(9),
      if(bookReviewTxtSplitted(10).isEmpty) List[String]() else bookReviewTxtSplitted(10).split('|').distinct.toList,
      bookReviewTxtSplitted(11))
  }
}

