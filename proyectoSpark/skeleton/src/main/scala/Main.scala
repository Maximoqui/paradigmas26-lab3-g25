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

    // ===================== ejercicio 3: pipeline map-reduce =====================

    // el diccionario vive en el driver; lo broadcasteamos para que cada worker
    // reciba una sola copia eficiente en vez de serializarlo con cada tarea
    val dictionaryBroadcast = sc.broadcast(dictionary)

    // a) flatMap: extraemos entidades del título y del cuerpo de cada post
    val entitiesRDD = filteredPostsRDD.flatMap { post =>
      val dict          = dictionaryBroadcast.value
      val titleEntities = Analyzer.detectEntities(post.title, dict)
      val bodyEntities  = Analyzer.detectEntities(post.selftext, dict)
      titleEntities ++ bodyEntities
    }

    // b) map: convertimos cada entidad en un par ((tipo, nombre), 1) listo para reducir
    val entityPairsRDD = entitiesRDD.map { entity =>
      ((entity.entityType, entity.text), 1)
    }

    // c) reduceByKey: sumamos los conteos parciales de cada clave para obtener el total por entidad
    val entityCountsRDD = entityPairsRDD.reduceByKey(_ + _)

    // d) traemos los resultados al driver, los ordenamos y mostramos el top K
    val entityCountsList = entityCountsRDD.collect().toList

    val topK = cmdArgs.topK

    val sortedTopK = entityCountsList
      .sortBy { case ((entityType, entityName), count) => (-count, entityType, entityName) }
      .take(topK)

    val topKMap = sortedTopK.toMap

    println(Formatters.formatEntityStats(topKMap, topK))
    println()

    // resumen de cuántas entidades hay por tipo (calculado en el driver sobre la lista ya recolectada)
    val typeCountsMap = entityCountsList
      .groupBy { case ((entityType, _), _) => entityType }
      .view
      .mapValues(entries => entries.map(_._2).sum)
      .toMap

    val totalEntities      = typeCountsMap.values.sum
    val typeStatsWithTotal = typeCountsMap + ("total" -> totalEntities)

    println(Formatters.formatTypeStats(typeStatsWithTotal))
    println()

    spark.stop()
  }
}