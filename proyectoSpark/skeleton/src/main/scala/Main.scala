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
    
    val inicio2 = System.currentTimeMillis()
    val filteredCount = filteredPostsRDD.count()
    val fin2 = System.currentTimeMillis()
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
    postsRDD.unpersist() // liberamos memoria del RDD de posts, ya no lo necesitamos para nada más
    filteredPostsRDD.unpersist() // liberamos memoria del RDD de posts filtrados, ya no lo necesitamos para nada más

    println(Formatters.formatTypeStats(typeStatsWithTotal))
    println()
    println(s"Tiempo para contar filteredPosts: ${fin2 - inicio2} ms")
    println(s"Tiempo para contar totalPosts: ${fin - inicio} ms")
    

    spark.stop()
  }
}