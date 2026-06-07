import org.apache.spark.sql.SparkSession // Permite crear y usar Spark

object Main {

  def main(args: Array[String]): Unit = { // Punto de entrada del programa

    // Parsea los argumentos de línea de comandos
    val cmdArgs = CommandLineArgs.parse(args) match {

      case Some(parsed) => parsed // Si son válidos, los guarda

      case None => return // Si son inválidos, termina el programa

    }

    // Crea una sesión de Spark
    val spark = SparkSession.builder()
      .appName("RedditNER") // Nombre de la aplicación
      .master("local[*]") // Usa todos los núcleos disponibles
      .getOrCreate() // Crea la sesión o reutiliza una existente

    val sc = spark.sparkContext // Obtiene el SparkContext

    // Lee el archivo de suscripciones
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile) match {

      case Left(error) =>
        println(error) // Muestra el error
        spark.stop() // Detiene Spark
        return // Finaliza el programa

      case Right(opts) => opts // Obtiene las suscripciones válidas

    }

    // Elimina los None y conserva solo las suscripciones válidas
    val subscriptions = subscriptionOpts.flatten

    // Verifica que exista al menos una suscripción válida
    if (subscriptions.isEmpty) {

      println("Error: No valid subscriptions found")
      spark.stop()
      return

    }

    // Obtiene la ruta del directorio de entidades
    val entitiesDir = cmdArgs.entitiesDir

    // Verifica que exista el directorio
    if (!new java.io.File(entitiesDir).exists()) {

      println(s"Error: entities directory '$entitiesDir' not found")
      spark.stop()
      return

    }

    // Carga todos los diccionarios de entidades una sola vez
    val dictionary = Dictionary.loadAll(entitiesDir)

    // Convierte la lista de suscripciones en un RDD distribuido
    val subsRDD = sc.parallelize(subscriptions)

    // Descarga los feeds en paralelo
    val postsRDD = subsRDD.flatMap { subscription =>

      // Descarga el contenido RSS/JSON de la suscripción
      val feedOpt = FileIO.downloadFeed(subscription.url)

      feedOpt match {

        case None =>

          // Si falla la descarga muestra advertencia
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")

          List.empty[Post] // Devuelve lista vacía

        case Some(content) =>

          // Convierte el JSON descargado en posts
          val posts = JsonParser.parsePosts(content, subscription.name)

          // Devuelve la lista de posts encontrados
          posts

      }

    }

    // Conserva solo posts con título y texto no vacíos
    val filteredPostsRDD = postsRDD.filter { post =>

      post.title.nonEmpty && post.selftext.nonEmpty

    }

    // Cuenta todos los posts descargados
    val totalPosts = postsRDD.count()

    // Cuenta los posts que pasaron el filtro
    val filteredCount = filteredPostsRDD.count()

    // Calcula cuántos fueron descartados
    val droppedCount = totalPosts - filteredCount

    // Calcula el promedio de caracteres por post
    val avgChars: Long =
      if (filteredCount > 0)

        filteredPostsRDD
          .map(p => (p.title.length + p.selftext.length).toLong) // Longitud total del post
          .sum() / filteredCount // Promedio

      else 0L // Si no hay posts válidos

    // Cantidad de feeds procesados correctamente
    val feedsSuccess = subscriptions.length

    // Cantidad de feeds fallidos
    val feedsFailed = 0

    // Mapa con estadísticas para mostrar
    val stats = Map(

      "feedsSuccess" -> feedsSuccess,
      "feedsFailed" -> feedsFailed,
      "postsSuccess" -> totalPosts.toInt,
      "postsFailed" -> 0,
      "postsFiltered" -> droppedCount.toInt,
      "avgChars" -> avgChars.toInt

    )

    // Imprime las estadísticas
    println(Formatters.formatProcessingStats(stats))

    println() // Línea en blanco

    // Si no quedó ningún post válido
    if (filteredCount == 0) {

      println("Error: No valid posts downloaded after filtering")
      spark.stop()
      return

    }

    // filteredPostsRDD queda listo para el ejercicio 3

  }
}