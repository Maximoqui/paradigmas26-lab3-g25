/* import org.apache.spark.sql.SparkSession
// SparkSession es el punto de entrada a Spark.
// Sin esto no podemos crear RDDs ni usar ninguna función distribuida.

object Main {

  def main(args: Array[String]): Unit = {

    // ── PARSEO DE ARGUMENTOS ─────────────────────────────────────────
    // Esto no tiene nada de Spark. Es código normal que lee los
    // argumentos que el usuario pasa por línea de comandos
    // (ruta del archivo de suscripciones, directorio de entidades, etc.)
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt ya imprime el error, solo salimos
    }

    // ── CREAR SPARK SESSION ──────────────────────────────────────────
    // SparkSession es el "chef jefe" del sistema distribuido.
    // Todo lo que Spark hace pasa a través de este objeto.
    //
    // .master("local[*]") significa: corré en esta misma máquina
    // usando TODOS los núcleos de CPU disponibles como workers.
    // En producción esto sería la URL de un cluster real.
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()

    // SparkContext es el objeto con el que creamos RDDs.
    // Lo obtenemos de la SparkSession.
    val sc = spark.sparkContext

    // ── CÓDIGO DEL DRIVER (antes de Spark) ──────────────────────────
    // Todo lo que sigue hasta sc.parallelize() corre en el DRIVER,
    // es decir, en un solo hilo, igual que el esqueleto original.
    // Spark todavía no interviene acá.

    val filePath = cmdArgs.subscriptionFile

    // Verificamos si el archivo existe ANTES de leerlo para poder
    // dar un mensaje de error descriptivo según la causa del fallo.
    val subscriptionOpts = if (!new java.io.File(filePath).exists()) {
      println(s"Error: Could not load $filePath - file not found")
      spark.stop() // Siempre cerrar Spark antes de salir
      return
    } else {
      FileIO.readSubscriptions(filePath) match {
        case None =>
          // El archivo existe pero no pudo parsearse: JSON inválido
          println(s"Error: Could not load $filePath - invalid JSON format")
          spark.stop()
          return
        case Some(opts) => opts
        // opts es List[Option[Subscription]]: cada elemento es
        // Some(sub) si era válido o None si le faltaba name/url
        // (FileIO ya imprimió el Warning por cada None)
      }
    }

    // .flatten convierte List[Option[Subscription]] en List[Subscription]
    // descartando los None (suscripciones malformadas)
    val subscriptions = subscriptionOpts.flatten

    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      spark.stop()
      return
    }

    // Verificamos el directorio de diccionarios antes de distribuir trabajo.
    // Si no existe, no tiene sentido descargar nada.
    val entitiesDir = cmdArgs.entitiesDir

    if (!new java.io.File(entitiesDir).exists()) {
      println(s"Error: entities directory '$entitiesDir' not found")
      spark.stop()
      return
    }

    // ── CARGA DEL DICCIONARIO EN EL DRIVER ──────────────────────────
    // Cargamos el diccionario UNA SOLA VEZ acá en el driver.
    //
    // ¿Por qué no dentro del flatMap más abajo?
    // Porque si lo cargáramos dentro del flatMap, cada worker
    // leería los archivos del disco por CADA post procesado,
    // lo que serían miles de lecturas innecesarias.
    //
    // Al cargarlo acá, Spark serializa el objeto una sola vez
    // y lo envía a cada worker junto con la función del flatMap.
    val dictionary = Dictionary.loadAll(entitiesDir)

    // ── SPARK EMPIEZA ACÁ ────────────────────────────────────────────
    // sc.parallelize() convierte una colección normal de Scala
    // en un RDD (Resilient Distributed Dataset).
    //
    // Un RDD es una colección distribuida e inmutable que Spark
    // puede repartir entre múltiples workers para procesarla en paralelo.
    // Analogía: es como repartir las fichas de trabajo entre los cocineros.
    val subsRDD = sc.parallelize(subscriptions)
    // subsRDD: RDD[Subscription]

    // ── TRANSFORMACIÓN: flatMap (WORKERS) ────────────────────────────
    // flatMap distribuye el trabajo entre los workers.
    // Cada worker recibe UNA suscripción y devuelve MUCHOS posts
    // (de ahí "flat": aplana todas las listas en un solo RDD).
    //
    // Usamos flatMap y no map porque:
    // - map: 1 entrada → exactamente 1 salida
    // - flatMap: 1 entrada → 0 o N salidas (cada URL da N posts)
    //
    // IMPORTANTE: el manejo de errores está DENTRO de la función.
    // Si una excepción se propagara afuera, Spark cancelaría el
    // job completo. Con List.empty[], ese worker no aporta posts
    // pero el resto del pipeline sigue funcionando.
    //
    // ATENCIÓN: las transformaciones de Spark son LAZY (perezosas).
    // Este flatMap NO se ejecuta todavía. Solo define qué hay que hacer.
    // Se ejecutará recién cuando llegue a una "acción" como .count()
    val postsRDD = subsRDD.flatMap { subscription =>
      val feedOpt = FileIO.downloadFeed(subscription.url)
      feedOpt match {
        case None =>
          // downloadFeed devuelve None si hubo error de red o timeout
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          List.empty[Post] // Lista vacía: este worker no aporta posts
        case Some(content) =>
          // parsePosts también maneja sus errores internamente
          // y devuelve List() si el JSON está malformado
          JsonParser.parsePosts(content, subscription.name)
      }
    }
    // postsRDD: RDD[Post] — todos los posts de todos los feeds

    // ── TRANSFORMACIÓN: filter (WORKERS) ────────────────────────────
    // filter es otra transformación lazy: cada worker evalúa
    // sus posts de forma independiente y descarta los vacíos.
    // No hay comunicación entre workers en este paso.
    val filteredPostsRDD = postsRDD.filter { post =>
      post.title.nonEmpty && post.selftext.nonEmpty
    }
    // filteredPostsRDD: RDD[Post] — solo posts con contenido útil

    // ── ACCIONES: count() ────────────────────────────────────────────
    // Acá Spark ejecuta REALMENTE el pipeline por primera vez.
    // Una "acción" es cualquier operación que necesita el resultado
    // concreto (no solo la receta). count() dispara la ejecución
    // de todos los flatMap y filter definidos arriba.
    //
    // NOTA para ejercicio 5: cada .count() re-ejecuta todo el pipeline
    // desde la descarga HTTP. Con .cache() evitaremos eso.
    val totalPosts    = postsRDD.count()         // dispara descarga + parseo
    val filteredCount = filteredPostsRDD.count() // dispara descarga + parseo + filter
    val droppedCount  = totalPosts - filteredCount

    // Calculamos el promedio de caracteres por post
    val avgChars: Long =
      if (filteredCount > 0)
        filteredPostsRDD
          .map(p => (p.title.length + p.selftext.length).toLong)
          .sum() / filteredCount
      else 0L

    // feedsSuccess y feedsFailed son placeholders por ahora.
    // En el ejercicio 4 estos valores vendrán de Accumulators,
    // que es el mecanismo de Spark para que los workers reporten
    // métricas al driver de forma segura.
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

    // Reutilizamos Formatters del esqueleto para que el output
    // sea idéntico al de la versión secuencial original
    println(Formatters.formatProcessingStats(stats))
    println()

    // ── CASO DE ERROR: sin posts válidos ─────────────────────────────
    if (filteredCount == 0) {
      println("Error: No valid posts downloaded after filtering")
      spark.stop()
      return
    }

    // ── filteredPostsRDD queda listo para el ejercicio 3 ─────────────
    // En el siguiente ejercicio aplicaremos Map-Reduce sobre este RDD
    // para extraer y contar las entidades nombradas en paralelo.

  }
} */