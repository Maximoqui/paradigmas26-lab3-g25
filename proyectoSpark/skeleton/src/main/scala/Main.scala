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

    // Definimos acumuladores para contar feeds y posts exitosos-descargados/fallidos-descartados
    val successfulFeeds = sc.longAccumulator("successfulFeeds")
    val failedFeeds = sc.longAccumulator("failedFeeds")
    val totalPosts = sc.longAccumulator("totalPosts")
    val totalPostsFailed = sc.longAccumulator("totalPostsFailed")


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
          // Si falla la descarga, actualizamos el contador de feeds fallidos
          failedFeeds.add(1)
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          List.empty[Post]
        case Some(content) =>
          // Si se descarga correctamente, actualizamos el contador de feeds exitosos
          successfulFeeds.add(1)
          JsonParser.parsePosts(content, subscription.name)
      }
    }.cache() // Cacheamos el RDD de posts para evitar recomputaciones, al ser llamados de nuevo para las estadísticas

    // Aplicamos el filtro a los posts descargados y contamos los resultados usando los acumuladores
    val filteredPostsRDD = postsRDD.filter { post =>

      totalPosts.add(1) // Contamos cada post procesado
      val valid = post.title.nonEmpty && post.selftext.nonEmpty
      
      if(!valid)
        totalPostsFailed.add(1) // Contamos cada post que no pasa el filtro
      
      valid
    }.cache() // Cacheamos el RDD para evitar recomputaciones, al ser llamados de nuevo para las estadísticas
    
    // Calculamos tiempos y estadísticas
    val inicio = System.currentTimeMillis()
    val totalPostsCount = postsRDD.count()
    val fin = System.currentTimeMillis()
    println(s"Tiempo para contar totalPosts: ${fin - inicio} ms")
    
    val inicio2 = System.currentTimeMillis()
    val filteredCount = filteredPostsRDD.count()
    val fin2 = System.currentTimeMillis()
    println(s"Tiempo para contar filteredPosts: ${fin2 - inicio2} ms")
    
    val droppedCount  = totalPostsCount - filteredCount
   
    val avgChars: Long =
      if (filteredCount > 0)
        filteredPostsRDD
          .map(p => (p.title.length + p.selftext.length).toLong)
          .sum() / filteredCount
      else 0L

    // Obtenemos los valores de los acumuladores para las estadísticas finales
    val feedsSuccess = successfulFeeds.value
    val feedsFailed  = failedFeeds.value

    val stats = Map(
      "feedsSuccess"  -> feedsSuccess,
      "feedsFailed"   -> feedsFailed,
      "postsSuccess"  -> totalPosts.value.toInt,
      "postsFailed"   -> totalPostsFailed.value.toInt,
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