import org.apache.spark.sql.SparkSession

object ExemploSparkMain {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("AulaSparkScalaDemo")
      .master("local[2]") // Utiliza 2 cores no seu computador
      .getOrCreate()

    // Importante para usar .toDF() com case classes ou tuplas
    import spark.implicits._

    val dados = Seq(
            ("Ana", 25, "SP"),
            ("Bruno", 30, "RJ"),
            ("Carla", 22, "MG")
    )

    val df = dados.toDF("Nome", "Idade", "Estado")

    println("--- Dados do DataFrame ---")
    df.show()

     // Exemplo de transformação simples (filtro)
     println("--- Pessoas com mais de 23 anos ---")
     df.filter($"Idade" > 23).show()

    spark.stop()

    }
}