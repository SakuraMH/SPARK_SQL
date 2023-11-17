package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;

public class Exr1 {

    public static void main(String[] args) {
        // Initialiser la session Spark
        SparkSession spark=SparkSession.builder().appName("TP SPARK SQL").master("local[*]")
                .getOrCreate();;

        // Charger les données depuis le fichier CSV
        String chemin_fichier = "incidents.csv";
        Dataset<Row> donnees_incidents = spark.read().csv(chemin_fichier).toDF("Id", "titre", "description", "service", "date");

        // Afficher le schéma des données
        donnees_incidents.printSchema();

        // 1. Afficher le nombre d'incidents par service
        Dataset<Row> nombre_incidents_par_service = donnees_incidents.groupBy("service").count();
        nombre_incidents_par_service.show();

        // 2. Afficher les deux années avec le plus grand nombre d'incidents
        Dataset<Row> donnees_incidents_avec_annee = donnees_incidents.withColumn("annee", functions.year(col("date")));
        Dataset<Row> nombre_incidents_par_annee = donnees_incidents_avec_annee.groupBy("annee").count().orderBy(col("count").desc());
        nombre_incidents_par_annee.show(2);

        // Arrêter la session Spark
        spark.stop();
    }
}
