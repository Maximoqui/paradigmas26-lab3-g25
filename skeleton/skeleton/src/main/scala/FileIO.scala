import scala.io.Source // Permite leer archivos y URLs
import org.json4s._ // Librería para trabajar con JSON
import org.json4s.jackson.JsonMethods._ // Métodos para parsear JSON

object FileIO { // Objeto que agrupa funciones de entrada/salida

  // Lee un archivo JSON de suscripciones
  // Devuelve None si el archivo no existe o el JSON es inválido
  def readSubscriptions(filePath: String): Option[List[Option[Subscription]]] = {
    implicit val formats: Formats = DefaultFormats // Configuración para convertir JSON a objetos Scala

    try { // Intenta ejecutar el código
      val file = new java.io.File(filePath) // Crea una referencia al archivo

      if (!file.exists()) return None // Si el archivo no existe, devuelve None

      val source = Source.fromFile(filePath) // Abre el archivo
      val content = source.mkString // Lee todo el contenido como String
      source.close() // Cierra el archivo

      val json = parse(content) // Convierte el texto a JSON
      val rawList = json.extract[List[Map[String, String]]] // Extrae una lista de mapas clave-valor

      val subscriptions = rawList.map { sub => // Recorre cada suscripción
        if (sub.contains("name") && sub.contains("url")) // Verifica que existan name y url
          Some(Subscription(sub("name"), sub("url"))) // Crea una suscripción válida
        else {
          println("Warning: Skipping malformed subscription (missing 'name' or 'url' field)") // Muestra advertencia
          None // Marca la suscripción como inválida
        }
      }

      Some(subscriptions) // Devuelve la lista de suscripciones

    } catch {
      case _: java.io.FileNotFoundException =>
        None // Archivo no encontrado

      case _: Exception =>
        None // JSON inválido u otro error
    }
  }

  // Descarga el contenido de una URL
  def downloadFeed(url: String): Option[String] = {
    try {
      val source = Source.fromURL(url) // Abre la URL
      val content = source.mkString // Lee todo el contenido
      source.close() // Cierra la conexión
      Some(content) // Devuelve el contenido descargado
    } catch {
      case _: Exception => None // Si ocurre un error, devuelve None
    }
  }

  // Lee un archivo de diccionario
  def readDictionaryFile(filePath: String): Option[List[String]] = {
    try {
      val source = Source.fromFile(filePath) // Abre el archivo

      val lines = source.getLines() // Obtiene las líneas del archivo
        .map(_.trim) // Elimina espacios al inicio y final
        .filter(_.nonEmpty) // Elimina líneas vacías
        .filterNot(_.startsWith("#")) // Ignora comentarios que empiezan con #
        .toList // Convierte las líneas a una lista

      source.close() // Cierra el archivo

      Some(lines) // Devuelve la lista de palabras
    } catch {
      case _: Exception => None // Si ocurre un error, devuelve None
    }
  }
}