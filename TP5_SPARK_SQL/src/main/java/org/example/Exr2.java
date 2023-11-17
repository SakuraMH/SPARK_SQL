package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class Exr2 {
    public static void main(String[] args) {
        SparkSession ss= SparkSession.builder().appName("les consultations de patients").master("local[*]").getOrCreate();

        // Charger la table des consultations
        Dataset<Row> dfConsultations = ss.read().format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")  // Utilisez la classe de pilote actualisée
                .option("url", "jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("dbtable", "consultations")  // Utilisez le nom correct de la table (consultation au lieu de PATIENTS)
                .option("user", "root")
                .option("password", "")
                .load();


        // Question 1   ///////////////////////////////////////////////////
        // Afficher le schéma pour vérifier la colonne de date
        dfConsultations.printSchema();

        // Convertir la colonne de date en format DateType
        Dataset<Row> dfWithDate = dfConsultations.withColumn("DATE_CONSULTATION", col("DATE_CONSULTATION").cast("date"));

        // Utiliser la fonction groupBy et count pour obtenir le nombre de consultations par jour
        Dataset<Row> consultationsParJour = dfWithDate.groupBy("DATE_CONSULTATION").agg(count("id").as("nombre_consultations"));

        // Afficher les résultats
        consultationsParJour.show();

        // Question2  ////////////////////////////////////////////
        // Charger la table des médecins
        Dataset<Row> dfMedecins = ss.read().format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/DB_HOPITAL")
                .option("dbtable", "medecins")
                .option("user", "root")
                .option("password", "")
                .load();

        // Joindre les tables sur la colonne id_medecin
        Dataset<Row> dfJoined = dfConsultations.join(dfMedecins, dfConsultations.col("id_medecin").equalTo(dfMedecins.col("id")).as("id_medecin"));
        dfJoined.show();
        // Utiliser la fonction groupBy et count pour obtenir le nombre de consultations par médecin
        Dataset<Row> consultationsParMedecin = dfJoined.groupBy("NOM", "PRENOM").agg(count("ID_MEDECIN").as("nombre_consultations"));

        // Afficher les résultats
        consultationsParMedecin.show();

        //Question 3 /////////////////////////////////////////

        // Utiliser la fonction groupBy et countDistinct pour obtenir le nombre de patients par médecin
        Dataset<Row> patientsParMedecin = dfJoined.groupBy("NOM", "PRENOM").agg(countDistinct("id_patient").as("nombre_patients"));

        // Afficher les résultats
        patientsParMedecin.show();

    }
}
