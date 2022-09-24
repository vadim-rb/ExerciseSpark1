import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex
///usr/hdp/2.6.1.0-129/spark2/bin/spark-submit --master yarn-cluster --class "Main" twtest2_2.11-0.1.0-SNAPSHOT.jar

object Main {

  //Find all the tweets by user
  def tweets_by_user(user: String)(implicit ss:SparkSession): DataFrame = {
    ss.sql(s"SELECT text FROM tweets WHERE user='${user}'")
  }

  //Find how many tweets(>2) each user has
  def counts_tweet_by_user(implicit ss:SparkSession): DataFrame = {
    ss.sql(s"SELECT user,count(*) FROM tweets group by user having count(*)>2 limit 10")
  }

  //Find all the persons mentioned on tweets !!! warn only small data
  def extract_(c: Seq[String], patternName: Regex)(implicit df: DataFrame, ss:SparkSession): DataFrame = {
    val lb: ListBuffer[(String, String)] = ListBuffer()
    val f = df.filter(col("text").rlike("(@[A-Za-z0-9_]+)")).collect()
    f.foreach(row => for (patternMatch <- patternName.findAllMatchIn(row.getAs("text"))) {
      lb += ((row.getAs("user"), patternMatch.toString()))
    }
    )
    import ss.implicits._
    val newdf = lb.toDF(c: _*)
    newdf
  }

  //Count how many times each person is mentioned
  def  count_(df: DataFrame,col_mention:String="person") : DataFrame = {
    df.groupBy(col_mention).count()
  }

  //Find the 10 most mentioned persons
  def top10_(df: DataFrame, col_count: String = "count") : DataFrame = {
    df.orderBy(col(col_count).desc).limit(10)
  }



  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("MySuperCode")
      .getOrCreate()
    val sc = spark.sparkContext
    implicit val df: DataFrame = spark.read.json("/user/ext-vadim.popov/reduced-tweets.json")
    df.createOrReplaceTempView("tweets")
    (1 to 5).foreach(println)
    println("start")

    println("*tweets_by_user*" * 5)
    println(tweets_by_user("hafizzul").collect().mkString("\n"))

    println("*counts_tweet_by_user*" * 5)
    println(counts_tweet_by_user.collect().mkString("\n"))

    println("*Find all the persons mentioned on tweets*" * 5)
    val patternName: Regex = "(@[A-Za-z0-9_]+)".r
    val columns_person = Seq("user", "person")
    val personDF = extract_(columns_person, patternName)
    personDF.show()

    println("*Count how many times each person is mentioned*" * 5)
    val countDF = count_(personDF)
    countDF.show()

    println("*Find the 10 most mentioned persons*" * 5)
    println(top10_(countDF).collect().mkString("\n"))

    println("*Find all the hashtags mentioned on a tweet*" * 5)
    val patternHashtag = "(#[A-Za-z0-9_]+)".r
    val columns_hashtag = Seq("user", "hashtag")
    val hashtagDF = extract_(columns_hashtag, patternHashtag)
    hashtagDF.show()

    println("*Count how many times each hashtag is mentioned*" * 5)
    val countHashDF = count_(hashtagDF,col_mention = "hashtag")
    countHashDF.show()

    println("*Find the 10 most popular Hashtags*" * 5)
    println(top10_(countHashDF).collect().mkString("\n"))

 }
}