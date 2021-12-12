import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
object twitter {

  def main(args: Array[String]): Unit = {


    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))

    // specify twitter consumerKey, consumerSecret, accessToken, accessTokenSecret
    val consumerKey = "N5IWiD7jWztbO6sktS0zr6Ct8"
    val consumerSecret = "3xBUQw9N2dWRrW4XxSyzlmI8MNS3AXG8Nsy3pdMnczUWsrIlwA"
    val token = "1357063042945712139-2glVWIqkAIBRuUP0S7DnVvUsphxSPe"
    val secret = "70Wrhz5T7uSoatFPCkzQaBU3NQmwWwL28Vw1wqqh3hfsq"


    System.setProperty("twitter4j.oauth.consumerKey",consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", token)
    System.setProperty("twitter4j.oauth.accessTokenSecret", secret)

    // Connect to Twitter and get the tweets object
    val filters = Array("covid 19")
    val tweets = TwitterUtils.createStream(ssc, None, filters)

    // Extract the text from the tweets object
    val tweets_collection = tweets.map(each_tweet => each_tweet.getText())
    tweets_collection.print()

    /*val englishTweets = tweets.filter(_.getLang() == "en")
    englishTweets.saveAsTextFiles("tweets", "csv")*/

    ssc.start()
    ssc.awaitTermination()
  }
}
