# Procesamiento distribuido con Apache Spark

## Ejercicio 1 - Identificar las regiones paralelizables

> a. Dibujen el diagrama de flujo de los pasos que tiene que hacer su programa
> (conexión, descarga, extracción de entidades, clasificación, conteo, ranking) como un
> grafo de dependencias (que seguramente será algo muy parecido a una secuencia).
> Cada uno de los pasos será una acción o transformación que realiza un worker o el
> driver. La conexión entre un paso A y un paso B es el output de A y el input de B. Explicite el tipo en Scala de cada conexión.

Diagrama de flujo del pipeline: se muestra el recorrido de los datos desde la lectura de las suscripciones hasta el conteo y clasificación de entidades, indicando los tipos de datos intercambiados entre cada etapa y la participación del driver (donde intervienen en el inicio para leer las suscripciones y coordinar la ejecución distribuida como también al final del pipeline para agrupar los resultados y mostrarlos) y los workers.
    ![pipeline](pipelineLAB3.png)

>b. Para cada paso del pipeline, determinen si puede expresarse como una de las abstracciones de Spark:
>- map: transforma cada elemento en exactamente un resultado. Aplicable cuando
>cada tarea es independiente y produce exactamente una salida.
>- flatMap: transforma cada elemento en cero o más resultados. Aplicable cuando
>cada tarea es independiente pero puede producir una cantidad variable de salidas.
>- reduceByKey u otra reducción: combina múltiples elementos en uno agrupando
>por clave. Aplicable cuando el resultado depende de todos los elementos, no de uno solo.
>¿Hay algún paso del pipeline que no encaje en ninguna de estas abstracciones? ¿Por qué?

Luego de realizar el gráfico,  se pudo identificar que la Contabilización de Entidades puede expresar con una de las abstracciones de Spark que es reduceByKey ya que se agrupan las entidades con la misma clave y se suma las ocurrencias para obtener el conteo total de cada una.
Por otro lado,  Descargar Feeds puede expresarse mediante flatmap ya que cada suscripción puede producir una cantidad variable de posts, es decir, puede no devolver ningun post valido o puede devolver varios posts, por lo que la relación ya es de muchos a muchos respecto a la entrada y salida, lo mismo pasa con la Extracción de Entidades ya que cada post puede obtener 0 o varias entidades nombradas.
Y map se utiliza para la Clasificación de Entidades porque transformamos cada entidad en un par clave-valor produciendo una salida por entidad. 
El caso donde no encaja con ninguna de las abstracciones que ofrece Spark seria cuando filtramos los post vacíos o que no cumplen con los requisitos pedidos, ya que estamos usando filter donde se encarga de conservar aquellos elementos que satisfacen la condición.

>c. Las reducciones constituyen una barrera de sincronización: ningún worker puede
>producir el resultado final hasta que todos hayan terminado su parte. Identifiquen qué
>pasos del pipeline son barreras y cuáles pueden ejecutarse de forma completamente
>independiente entre workers.

Los pasos de Descargar Feed, Filtrar Post, Extraer Entidades pueden ejecutarse de forma independiente entre los workers. Esto se debe porque son tareas que cada worker puede procesar los datos que le fueron asignados sin necesitar información de otro.
En cambio el paso barrera de sincronización viene a ser Clasificar y Contabilizar Errores  
ya que para obtener la cantidad de apariciones de cada entidad es necesario combinar los resultados generados por todos los workers.
Por ejemplo: si tenemos Worker 1 -> Scala = 2 Worker 2 -> Scala = 3 Worker 3 -> Scala = 1 
y queremos saber cuántas veces apareció Scala en total hay que juntar  los resultados de todos los workers (Scala = 6).


>d. El mecanismo de extensión (extension point) de Spark es la función que el
>desarrollador le pasa a cada transformación. ¿Qué restricciones impone Spark sobre
>esas funciones para que puedan ejecutarse en un entorno distribuido? Piensen en
>serialización, estado compartido y efectos secundarios.


Esta pregunta, reformulándola, se refiere a qué características debe tener una función para que Spark pueda ejecutarla en distintos workers de forma distribuida. Una de las principales restricciones es que dichas funciones deberían ser lo más puras posible, tal como se trabajó en el Laboratorio 1. Esto implica evitar depender de variables externas o de estado compartido, ya que cada worker ejecuta una copia independiente de la función. Además, Spark debe poder enviar estas funciones desde el driver hacia los workers para que puedan ejecutarse de manera distribuida.