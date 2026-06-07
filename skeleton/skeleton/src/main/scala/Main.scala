import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return
    }

    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val filePath = cmdArgs.subscriptionFile

    val subscriptionOpts = if (!new java.io.File(filePath).exists()) {
      println(s"Error: Could not load $filePath - file not found")
      spark.stop()
      return
    } else {
      FileIO.readSubscriptions(filePath) match {
        case None =>
          println(s"Error: Could not load $filePath - invalid JSON format")
          spark.stop()
          return
        case Some(opts) => opts
      }
    }

    
    val subscriptions = subscriptionOpts.flatten

    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      spark.stop()
      return
    }

    val entitiesDir = cmdArgs.entitiesDir

    if (!new java.io.File(entitiesDir).exists()) {
      println(s"Error: entities directory '$entitiesDir' not found")
      spark.stop()
      return
    }

    val dictionary = Dictionary.loadAll(entitiesDir)

    val subsRDD = sc.parallelize(subscriptions)

    val postsRDD = subsRDD.flatMap { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      feedOpt match {
        case None =>
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          List.empty[Post]
        case Some(content) =>
          JsonParser.parsePosts(content, subscription.name)
      }
    }

    val filteredPostsRDD = postsRDD.filter { post =>
      post.title.nonEmpty && post.selftext.nonEmpty
    }

    val totalPosts    = postsRDD.count()
    val filteredCount = filteredPostsRDD.count()
    val droppedCount  = totalPosts - filteredCount

    val avgChars: Long =
      if (filteredCount > 0)
        filteredPostsRDD
          .map(p => (p.title.length + p.selftext.length).toLong)
          .sum() / filteredCount
      else 0L

    val feedsSuccess = subscriptions.length
    val feedsFailed  = 0

    val stats = Map(
      "feedsSuccess"  -> feedsSuccess,
      "feedsFailed"   -> feedsFailed,
      "postsSuccess"  -> totalPosts.toInt,
      "postsFailed"   -> 0,
      "postsFiltered" -> droppedCount.toInt,
      "avgChars"      -> avgChars.toInt
    )

    println(Formatters.formatProcessingStats(stats))
    println()

    if (filteredCount == 0) {
      println("Error: No valid posts downloaded after filtering")
      spark.stop()
      return
    }

    // filteredPostsRDD listo para ejercicio 3
  }
}