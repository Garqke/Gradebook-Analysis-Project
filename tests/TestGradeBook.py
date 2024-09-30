import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import sys
import os
from pathlib import Path
from typing import Any, Dict

# Asegurar que el directorio padre está en el path para importar gradebook
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from gradebook import (
    load_and_format_csv, load_hw_grades_csv, 
    load_quiz_grades, combine_student_data, calculate_homework_scores, 
    calculate_quiz_scores, calculate_letter_grade
)

class TestGradebookFunctions(unittest.TestCase):
    """
    Unit test class for the gradebook module functions.
    """

    @classmethod
    def setUpClass(cls) -> None:
        """
        Sets up the Spark session before running the tests.
        """
        cls.spark: SparkSession = SparkSession.builder.master("local").appName("GradebookTest").getOrCreate()

    @classmethod
    def tearDownClass(cls) -> None:
        """
        Stops the Spark session after running the tests.
        """
        if cls.spark is not None:
            cls.spark.stop()
    
    @patch('gradebook.spark')
    def test_load_and_format_csv(self, mock_spark) -> None:
        """
        Tests the load_and_format_csv function to ensure it correctly transforms the names
        and handles debugging output when debug=True.
        """
        print("mock_read_csv ha sido llamado con:", mock_spark.read.csv.call_args)
        ls_data = [("John", 25), ("Jane", 30)]
        ls_columns = ["Name", "Age"]
        df_mock = self.spark.createDataFrame(ls_data, ls_columns)
        mock_spark.read.csv.return_value = df_mock

        transformations = {
            "NAME": F.upper(F.col("Name")),
            "AGE": F.col("Age") + 5
        }

        # Test sin debugging
        df_formatted, _ = load_and_format_csv("dummy_path.csv", transformations)
        
        # Verificar las columnas transformadas
        self.assertIn("NAME", df_formatted.columns, "Column 'NAME' does not exist in formatted df")
        self.assertIn("AGE", df_formatted.columns, "Column 'AGE' does not exist in formatted df")

        # Verificar los datos transformados
        data_collected = df_formatted.collect()
        self.assertEqual(len(data_collected), 2, "El número de filas no coincide.")

        # Primer registro
        self.assertEqual(data_collected[0]["NAME"], "JOHN")
        self.assertEqual(data_collected[0]["AGE"], 30)  # 25 + 5

        # Segundo registro
        self.assertEqual(data_collected[1]["NAME"], "JANE")
        self.assertEqual(data_collected[1]["AGE"], 35)  # 30 + 5

        # Test con debugging activado
        df_formatted_debug, debug_info = load_and_format_csv("dummy_path.csv", transformations, debug=True)

        # Verificar que el diccionario de debugging contiene los DataFrames intermedios
        self.assertIn("dfs", debug_info, "El diccionario de debug no contiene 'dfs'")
        self.assertEqual(len(debug_info["dfs"]), 3, "El número de DataFrames en debug no coincide.")
        
        # Verificar los DataFrames almacenados en debugging
        self.assertEqual(debug_info["dfs"][0].columns, df_mock.columns, "El DataFrame original no coincide en debug.")
        self.assertEqual(debug_info["dfs"][-1].columns, df_formatted.columns, "El DataFrame final no coincide en debug.")


    @patch('gradebook.spark')
    def test_load_hw_grades_csv_mock(self, mock_spark):
        """
        Tests the load_hw_grades_csv function using mocking to simulate file reading.
        """
        # Configuración del DataFrame de ejemplo que se devolverá cuando se llame a spark.read.csv
        data = [("abc123", "Jane", "Doe", 85, 90, "2023-09-01"),
                ("def456", "John", "Smith", 75, 80, "2023-09-02")]
        ls_columns = ["SID", "First Name", "Last Name", "Homework 1", "Homework 2", "SUBMISSION Date"]
        mock_df = self.spark.createDataFrame(data, ls_columns)
        mock_spark.read.csv.return_value = mock_df

        # Llamar a la función con un dummy path sin debug
        df_hw, _ = load_hw_grades_csv("dummy_path.csv")

        # Verificar que solo se llamó 1 vez a mock_read_csv
        mock_spark.read.csv.assert_called_once_with("dummy_path.csv", header=True, inferSchema=True)

        expected_columns = {"SID", "FIRST_NAME", "LAST_NAME", "HOMEWORK_1", "HOMEWORK_2"}
        self.assertSetEqual(set(df_hw.columns), expected_columns)

        # Verificar los datos
        data_collected = df_hw.collect()
        self.assertEqual(len(data_collected), 2)
        self.assertEqual(data_collected[0]["SID"], "abc123")
        self.assertEqual(data_collected[0]["FIRST_NAME"], "Jane")
        self.assertEqual(data_collected[0]["LAST_NAME"], "Doe")
        self.assertEqual(data_collected[0]["HOMEWORK_1"], 85)
        self.assertEqual(data_collected[0]["HOMEWORK_2"], 90)

        # Probar la opción de debugging
        df_hw_debug, debug_info = load_hw_grades_csv("dummy_path.csv", debug=True)

        # Verificar que el diccionario de debugging contiene los DataFrames intermedios
        self.assertIn("dfs", debug_info, "El diccionario de debug no contiene 'dfs'")
        self.assertEqual(len(debug_info["dfs"]), 3, "El número de DataFrames en debug no coincide.")

        # Verificar que los DataFrames de debugging contienen las columnas correctas
        self.assertSetEqual(set(debug_info["dfs"][0].columns), set(mock_df.columns), "El DataFrame original no coincide en debug.")
        self.assertSetEqual(set(debug_info["dfs"][-1].columns), expected_columns, "El DataFrame final no coincide en debug.")


    @patch("gradebook.Path.glob")
    @patch("gradebook.spark")
    def test_load_quiz_grades_mock(self, mock_spark, mock_glob):
        """
        Tests the load_quiz_grades function using mocking to simulate file reading and globbing.
        """
        # dummy dirs
        ls_mock_file_paths = [
            Path("/fake_dir/quiz_1_grades.csv"),
            Path("/fake_dir/quiz_2_grades.csv"),
            Path("/fake_dir/quiz_3_grades.csv")
        ]
        mock_glob.return_value = ls_mock_file_paths

        # Configuración del mock y creación de DataFrames de ejemplo:
        quiz_1_data = [("jane@uni.es", "Jane", "Doe", 8),
                    ("john@uni.es", "John", "Smith", 7)]
        ls_quiz_1_columns = ["Email", "First Name", "Last Name", "Grade"]
        df_quiz_1 = self.spark.createDataFrame(quiz_1_data, ls_quiz_1_columns)

        quiz_2_data = [("jane@uni.es", "Jane", "Doe", 9),
                    ("john@uni.es", "John", "Smith", 6)]
        ls_quiz_2_columns = ["Email", "First Name", "Last Name", "Grade"]
        df_quiz_2 = self.spark.createDataFrame(quiz_2_data, ls_quiz_2_columns)

        quiz_3_data = [("jane@uni.es", "Jane", "Doe", 10),
                    ("john@uni.es", "John", "Smith", 9)]
        ls_quiz_3_columns = ["Email", "First Name", "Last Name", "Grade"]
        df_quiz_3 = self.spark.createDataFrame(quiz_3_data, ls_quiz_3_columns)

        # Orden de respuesta de spark.read.csv
        mock_spark.read.csv.side_effect = [df_quiz_1, df_quiz_2, df_quiz_3]

        # Llamar a la función con el dummy folder sin debug
        data_folder = "/fake_dir"
        df_quiz_grades, _ = load_quiz_grades(data_folder)

        # Verificar que Path.glob fue llamado correctamente
        mock_glob.assert_called_once_with("quiz_*_grades.csv")

        # Verificar que spark.read.csv fue llamado tres veces con los archivos correctos
        expected_calls = [
            unittest.mock.call("/fake_dir/quiz_1_grades.csv", header=True, inferSchema=True),
            unittest.mock.call("/fake_dir/quiz_2_grades.csv", header=True, inferSchema=True),
            unittest.mock.call("/fake_dir/quiz_3_grades.csv", header=True, inferSchema=True)
        ]
        mock_spark.read.csv.assert_has_calls(expected_calls, any_order=False)

        # Verificar que el DataFrame resultante tiene las columnas esperadas
        expected_columns = {"EMAIL", "FIRST_NAME", "LAST_NAME", "QUIZ_1_GRADES", "QUIZ_2_GRADES", "QUIZ_3_GRADES"}
        self.assertSetEqual(set(df_quiz_grades.columns), expected_columns, "Las columnas del DataFrame final no son las esperadas.")

        # Verificar los datos combinados
        data = df_quiz_grades.collect()
        self.assertEqual(len(data), 2, "El número de filas en el DataFrame final no es el esperado.")

        # Verificar los datos para Jane Doe
        jane_row = next(row for row in data if row["EMAIL"] == "jane@uni.es")
        self.assertEqual(jane_row["FIRST_NAME"], "Jane")
        self.assertEqual(jane_row["LAST_NAME"], "Doe")
        self.assertEqual(jane_row["QUIZ_1_GRADES"], 8)
        self.assertEqual(jane_row["QUIZ_2_GRADES"], 9)
        self.assertEqual(jane_row["QUIZ_3_GRADES"], 10)

        # Verificar los datos para John Smith
        john_row = next(row for row in data if row["EMAIL"] == "john@uni.es")
        self.assertEqual(john_row["FIRST_NAME"], "John")
        self.assertEqual(john_row["LAST_NAME"], "Smith")
        self.assertEqual(john_row["QUIZ_1_GRADES"], 7)
        self.assertEqual(john_row["QUIZ_2_GRADES"], 6)
        self.assertEqual(john_row["QUIZ_3_GRADES"], 9)

        # Orden de respuesta de spark.read.csv
        mock_spark.read.csv.side_effect = [df_quiz_1, df_quiz_2, df_quiz_3]

        # Probar la opción de debugging
        df_quiz_grades_debug, debug_info = load_quiz_grades(data_folder, debug=True)

        # Verificar que spark.read.csv fue llamado tres veces con los archivos correctos
        expected_calls = [
            unittest.mock.call("/fake_dir/quiz_1_grades.csv", header=True, inferSchema=True),
            unittest.mock.call("/fake_dir/quiz_2_grades.csv", header=True, inferSchema=True),
            unittest.mock.call("/fake_dir/quiz_3_grades.csv", header=True, inferSchema=True)
        ]
        mock_spark.read.csv.assert_has_calls(expected_calls, any_order=False)

        # Verificar que el diccionario de debugging contiene los DataFrames intermedios
        self.assertIn("dfs", debug_info, "El diccionario de debug no contiene 'dfs'")
        self.assertEqual(len(debug_info["dfs"]), 12, "El número de DataFrames en debug no coincide.")

        # Verificar que los DataFrames de debugging contienen las columnas correctas
        self.assertSetEqual(set(debug_info["dfs"][0].columns), set(df_quiz_1.columns), "El DataFrame del primer quiz no coincide en debug.")
        self.assertSetEqual(set(debug_info["dfs"][-1].columns), expected_columns, "El DataFrame final no coincide en debug.")



    def test_combine_student_data(self) -> None:
        """
        Tests the combine_student_data function to ensure it correctly combines the roster, homework, and quiz DataFrames.
        """
        # Crear DataFrame de roster
        df_roster = self.spark.createDataFrame(
            [("abc123 ", " jane@uni.es ", "Section 1")],
            ["NetID", "Email Address", "SECTION"]
        )
        
        # Aplicar las mismas transformaciones que en load_and_format_csv
        df_roster_transformations = {
            "NetID": F.upper(F.regexp_replace(F.col("NetID"), r'\s+', '')),
            "EMAIL": F.upper(F.regexp_replace(F.col("Email Address"), r'\s+', ''))
        }
        df_roster = df_roster.withColumn("NetID", df_roster_transformations["NetID"]) \
                            .withColumn("EMAIL", df_roster_transformations["EMAIL"]) \
                            .select("SECTION", "EMAIL", "NetID")
        
        # Crear DataFrame de tareas y exámenes
        df_hw = self.spark.createDataFrame(
            [("abc123", "Jane", "Doe", 95)],
            ["SID", "First Name", "Last Name", "Homework 1"]
        )
        
        # Simular la normalización de los nombres de las columnas en load_hw_grades_csv
        ls_new_column_names = [c.upper().replace(" ", "_") for c in df_hw.columns]
        df_hw = df_hw.toDF(*ls_new_column_names)
        
        # Seleccionar columnas relevantes
        df_hw = df_hw.select([c for c in df_hw.columns if "SUBMISSION" not in c])
        
        # Crear DataFrame de quizzes
        df_quiz = self.spark.createDataFrame(
            [("Doe", "Jane", " jane@uni.es", 10)],
            ["Last Name", "First Name", "Email", "Grade"]
        )
        
        # Normalizar los nombres de las columnas
        df_quiz = df_quiz.withColumnRenamed("Last Name", "LAST_NAME") \
                        .withColumnRenamed("First Name", "FIRST_NAME") \
                        .withColumnRenamed("Email", "EMAIL") \
                        .withColumnRenamed("Grade", "GRADE")
        
        # Aplicar transformaciones específicas de load_quiz_grades
        df_quiz = df_quiz.withColumn("EMAIL", F.lower(F.trim(F.col("EMAIL")))) \
                        .withColumn("QUIZ_1_GRADES", F.col("GRADE")) \
                        .drop("GRADE")
        
        # Transformaciones para homework y roster
        df_roster = df_roster.withColumn("EMAIL", F.lower(F.trim(F.col("EMAIL")))) \
                            .withColumn("NetID", F.lower(F.trim(F.col("NetID"))))
        
        df_hw = df_hw.withColumn("SID", F.lower(F.trim(F.col("SID"))))
        
        # Llamamos a la función combine_student_data sin debug
        df_combined, _ = combine_student_data(df_roster, df_hw, df_quiz)
        
        # Verificar los resultados sin debug
        self.assertEqual(df_combined.count(), 1)
        self.assertIn("HOMEWORK_1", df_combined.columns)
        self.assertIn("QUIZ_1_GRADES", df_combined.columns)
        
        # Llamamos a la función combine_student_data con debug activado
        df_combined_debug, debug_info = combine_student_data(df_roster, df_hw, df_quiz, debug=True)

        # Verificar que el diccionario de debugging contiene los DataFrames intermedios
        self.assertIn("dfs", debug_info, "El diccionario de debug no contiene 'dfs'")
        self.assertEqual(len(debug_info["dfs"]), 4, "El número de DataFrames en debug no coincide.")
        
        # Verificar los DataFrames almacenados en debugging
        self.assertEqual(debug_info["dfs"][0].columns, df_roster.columns, "El DataFrame de roster no coincide en debug.")
        self.assertEqual(debug_info["dfs"][-1].columns, df_combined.columns, "El DataFrame combinado no coincide en debug.")


    def test_calculate_homework_scores(self) -> None:
        """
        Tests the calculate_homework_scores function to ensure it correctly calculates HOMEWORK_SCORE.
        """
        # Crear DataFrame de entrada
        df = self.spark.createDataFrame([
            (1, 80, 90, 70, 10, 10, 10),
            (2, 70, 80, 90, 10, 10, 10)
        ], 
        ["SID", "HOMEWORK_1", "HOMEWORK_2", "HOMEWORK_3", "HOMEWORK_1_-_MAX_POINTS", "HOMEWORK_2_-_MAX_POINTS", "HOMEWORK_3_-_MAX_POINTS"]
        )
        df_result, _ = calculate_homework_scores(df)

        self.assertIn("HOMEWORK_SCORE", df_result.columns)
        self.assertGreater(df_result.filter(F.col("SID") == 1).select("HOMEWORK_SCORE").first()["HOMEWORK_SCORE"], 0)

        # Probar la opción de debugging
        df_result_debug, debug_info = calculate_homework_scores(df, debug=True)
        self.assertIn("dfs", debug_info)
        self.assertEqual(len(debug_info["dfs"]), 5)  # Asegurarse de que haya 4 DataFrames intermedios

    def test_calculate_homework_scores_with_missing_data(self) -> None:
        """
        Tests the calculate_homework_scores function to ensure it correctly handles None values in HOMEWORK.
        """
        # Crear DataFrame con algunos valores None en las tareas
        df = self.spark.createDataFrame([
            (1, 80, None, 70, 10, 10, 10),
            (2, None, 80, 90, 10, 10, 10)
        ], 
        ["SID", "HOMEWORK_1", "HOMEWORK_2", "HOMEWORK_3", "HOMEWORK_1_-_MAX_POINTS", "HOMEWORK_2_-_MAX_POINTS", "HOMEWORK_3_-_MAX_POINTS"]
        )
        df_result, _ = calculate_homework_scores(df)

        self.assertIn("HOMEWORK_SCORE", df_result.columns)
        score1 = df_result.filter(F.col("SID") == 1).select("HOMEWORK_SCORE").first()["HOMEWORK_SCORE"]
        score2 = df_result.filter(F.col("SID") == 2).select("HOMEWORK_SCORE").first()["HOMEWORK_SCORE"]

        self.assertGreaterEqual(score1, 0)
        self.assertGreaterEqual(score2, 0)

        # Probar la opción de debugging
        df_result_debug, debug_info = calculate_homework_scores(df, debug=True)
        self.assertIn("dfs", debug_info)
        self.assertEqual(len(debug_info["dfs"]), 5)

    def test_calculate_quiz_scores(self) -> None:
        """
        Tests the calculate_quiz_scores function to ensure it correctly calculates QUIZ_SCORE.
        """
        # Crear DataFrame de entrada
        df = self.spark.createDataFrame([
            (1, 8, 7, 9, 6, 8),
            (2, 6, 5, 9, 9, 7)
        ],
        ["SID", "QUIZ_1_GRADES", "QUIZ_2_GRADES", "QUIZ_3_GRADES", "QUIZ_4_GRADES", "QUIZ_5_GRADES"]
        )
        df_result, _ = calculate_quiz_scores(df)

        self.assertIn("QUIZ_SCORE", df_result.columns)
        self.assertGreater(df_result.filter(F.col("SID") == 1).select("QUIZ_SCORE").first()["QUIZ_SCORE"], 0)

        # Probar la opción de debugging
        df_result_debug, debug_info = calculate_quiz_scores(df, debug=True)
        self.assertIn("dfs", debug_info)
        self.assertEqual(len(debug_info["dfs"]), 7)

    def test_calculate_letter_grade(self) -> None:
        """
        Tests the calculate_letter_grade function to ensure it correctly assigns final grades.
        """
        # Crear DataFrame de entrada
        df = self.spark.createDataFrame([
            (1, 0.9, 0.85, 0.7, 0.95, 0.8),
            (2, 0.6, 0.65, 0.7, 0.75, 0.7)
        ],
        ["SID", "EXAM_1_SCORE", "EXAM_2_SCORE", "EXAM_3_SCORE", "QUIZ_SCORE", "HOMEWORK_SCORE"]
        )
        df_result, _ = calculate_letter_grade(df)

        self.assertIn("FINAL_GRADE", df_result.columns)
        self.assertEqual(df_result.filter(F.col("SID") == 1). select("FINAL_GRADE").first()["FINAL_GRADE"], "B")
        self.assertEqual(df_result.filter(F.col("SID") == 2). select("FINAL_GRADE").first()["FINAL_GRADE"], "C")

        # Probar la opción de debugging
        df_result_debug, debug_info = calculate_letter_grade(df, debug=True)
        self.assertIn("dfs", debug_info)
        self.assertEqual(len(debug_info["dfs"]), 4)


if __name__ == '__main__':
    unittest.main()
