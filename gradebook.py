#!/usr/bin/env python
# coding: utf-8

# In[8]:


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col
from functools import reduce
from pathlib import Path
import re
import operator


DATA_FOLDER = Path("./input_data")


# In[9]:


spark = SparkSession.builder.appName("gradeBook").getOrCreate()


# # Load Data

# In[10]:


def load_and_format_csv(file_path: str, transformations: dict, debug=False) -> DataFrame:
    """
    Loads a CSV file and applies the specified transformations to the columns.
    
    :param file_path: The path to the CSV file.
    :param transformations: A dictionary with columns to transform and their corresponding expressions.
    :param debug: Boolean flag to enable debugging mode.
    :return: DataFrame with the applied transformations and a dictionary for debugging if debug=True.
    """
    
    lst_dfs = []
    lst_params = [file_path, transformations]
    
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    lst_dfs.append(df)  # Add original df to lst_dfs for debugging
    
    for col_name, transformation in transformations.items():
        df = df.withColumn(col_name, transformation)
        lst_dfs.append(df)
    
    dict_debug = {"dfs": lst_dfs, "params": lst_params} if debug else {}

    return df, dict_debug


# In[11]:


def load_hw_grades_csv(file_path: str, debug=False) -> DataFrame:
    """
    Loads and formats the CSV file containing homework and exam grades.
    
    :param file_path: The path to the CSV file.
    :param debug: Boolean flag to enable debugging mode.
    :return: DataFrame with renamed and filtered columns and a dictionary for debugging if debug=True.
    """
    
    lst_dfs = []
    lst_params = [file_path]
    
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    lst_dfs.append(df)  # Add original df for debugging

    new_column_names = [c.upper().replace(" ", "_") for c in df.columns]
    df = df.toDF(*new_column_names)
    lst_dfs.append(df)  # Add df after renaming columns

    df = df.select([c for c in df.columns if "SUBMISSION" not in c])
    lst_dfs.append(df)  # Add df after filtering columns
    
    dict_debug = {"dfs": lst_dfs, "params": lst_params} if debug else {}

    return df, dict_debug


# In[12]:


def load_quiz_grades(data_folder: str, debug=False) -> DataFrame:
    """
    Loads and combines all quiz files into a single DataFrame.
    
    :param data_folder: Folder where the quiz files are located.
    :param debug: Boolean flag to enable debugging mode.
    :return: DataFrame containing the quiz grades and a dictionary for debugging if debug=True.
    """
    
    lst_dfs = []
    lst_params = [data_folder]
    
    df_quiz_grades = None
    for file_path in DATA_FOLDER.glob("quiz_*_grades.csv"):
        quiz_name = file_path.stem.lower().replace(" ", "_")
        df_quiz = spark.read.csv(f"{data_folder}/{quiz_name}.csv", header=True, inferSchema=True)
        lst_dfs.append(df_quiz)  # Add original df for debugging
        
        df_quiz_mod = df_quiz.withColumn("EMAIL", F.lower(F.col("Email"))) \
                             .withColumnRenamed("First Name", "FIRST_NAME") \
                             .withColumnRenamed("Last Name", "LAST_NAME") \
                             .select("EMAIL", "GRADE", "FIRST_NAME", "LAST_NAME")
        lst_dfs.append(df_quiz_mod)  # Add modified df for debugging
        
        df_quiz_final = df_quiz_mod.withColumn(quiz_name.upper(), F.col("GRADE")).drop("GRADE")
        lst_dfs.append(df_quiz_final)  # Add final quiz df for debugging
        
        # Si es la primera iteracion guardamos el primer resultado en lo que será el df final
        if df_quiz_grades is None:
            df_quiz_grades = df_quiz_final
        else:
            df_quiz_grades = df_quiz_grades.join(df_quiz_final, on=["EMAIL", "FIRST_NAME", "LAST_NAME"], how="outer")
        lst_dfs.append(df_quiz_grades)  # Add combined df for debugging
    
    dict_debug = {"dfs": lst_dfs, "params": lst_params} if debug else {}

    return df_quiz_grades, dict_debug


# In[13]:


df_roster_transformations = {
    "NETID": F.upper(F.regexp_replace(F.col("NetID"), r'\s+', '')),
    "EMAIL": F.upper(F.regexp_replace(F.col("Email Address"), r'\s+', ''))
}

df_roster_res = load_and_format_csv(DATA_FOLDER.stem.title() + "/roster.csv", df_roster_transformations)[0].select("SECTION", "EMAIL", "NETID")



# In[14]:


df_hw_exam_grades = load_hw_grades_csv(DATA_FOLDER.stem.title() + "/hw_exam_grades.csv")[0]



# In[15]:


df_quiz_grades = load_quiz_grades(DATA_FOLDER.stem.lower())[0]


# # Join DataFrames

# In[16]:


def combine_student_data(df_roster: DataFrame, df_hw: DataFrame, df_quiz: DataFrame, debug=False) -> DataFrame:
    """
    Combines student data from the roster, homework/exams, and quizzes.
    
    :param df_roster: DataFrame containing student roster data.
    :param df_hw: DataFrame containing homework and exam grades.
    :param df_quiz: DataFrame containing quiz grades.
    :param debug: Boolean flag to enable debugging mode.
    :return: Combined DataFrame with all joined columns and a dictionary for debugging if debug=True.
    """
    
    lst_dfs = []
    lst_params = [df_roster, df_hw, df_quiz]
    
    # Transformar campos NetID, SID y EMAIL a minuscula
    df_roster = df_roster.withColumn("NetID", F.lower(F.col("NetID"))).withColumn("EMAIL", F.lower(F.col("EMAIL")))
    lst_dfs.append(df_roster)  # Add df_roster after transformation for debugging
    
    df_hw = df_hw.withColumn("SID", F.lower(F.col("SID")))
    lst_dfs.append(df_hw)  # Add df_hw after transformation for debugging

    # Join roster con hw usando NetID y SID
    df_combined = df_roster.join(df_hw, df_roster["NetID"] == df_hw["SID"], how="inner").drop("NetID")
    lst_dfs.append(df_combined)  # Add df_combined after first join for debugging

    # Join del resultado anterior con las notas de quizzes usando First Name, Last Name y Email.
    df_final_combined = df_combined.join(df_quiz, on=["FIRST_NAME", "LAST_NAME", "EMAIL"], how="inner").fillna(0)
    lst_dfs.append(df_final_combined)  # Add df_final_combined after second join for debugging
    
    dict_debug = {"dfs": lst_dfs, "params": lst_params} if debug else {}

    return df_final_combined, dict_debug


# In[17]:


df_final_combined = combine_student_data(df_roster_res, df_hw_exam_grades, df_quiz_grades)[0]



# # Calculate variables

# ## Calculate Exam Total Score

# In[18]:


n_exams = 3
df_final_exam_score = None

for n in range(1, n_exams + 1):
    exam_col = f"EXAM_{n}"
    max_points_col = f"EXAM_{n}_-_MAX_POINTS"
    score_col = f"EXAM_{n}_SCORE"

    if df_final_exam_score is None:
        df_final_exam_score = df_final_combined.withColumn(
            score_col,
            F.col(exam_col) / F.col(max_points_col)
        )
    else:
        df_final_exam_score = df_final_exam_score.withColumn(
            score_col,
            F.col(exam_col) / F.col(max_points_col)
        )



# ## Calculate HomeWork score

# In[19]:


def calculate_homework_scores(df_final_exam_score: DataFrame, debug=False) -> DataFrame:
    """
    Calculates homework scores based on total and average scores.
    Then, determines the best score between the total and the average.
    
    :param df_final_exam_score: DataFrame containing homework scores and maximum points.
    :param debug: Boolean flag to enable debugging mode.
    :return: DataFrame with added columns TOTAL_HOMEWORK, AVERAGE_HOMEWORK, and HOMEWORK_SCORE, along with debugging information if debug=True.
    """
    
    lst_dfs = []
    lst_params = [df_final_exam_score]

    # Obtener columnas
    columns = df_final_exam_score.columns

    # Filtrar las columnas por puntajes y máximos puntajes usando una expresión regular que permite múltiples dígitos
    homework_scores_columns = [
        col for col in columns if re.match(r"^HOMEWORK_\d+$", col)
    ]
    homework_max_points_columns = [
        col for col in columns if re.match(r"^HOMEWORK_\d+_-_MAX_POINTS$", col)
    ]
    
    lst_dfs.append(df_final_exam_score)  # Añadir df original a lst_dfs para debugging

    # Reemplazar valores None por 0 en los puntajes de hw
    for c in homework_scores_columns:
        df_final_exam_score = df_final_exam_score.withColumn(c, F.coalesce(F.col(c), F.lit(0)))
        # Reemplaza cualquier valor NULL en la columna de hw con 0
    lst_dfs.append(df_final_exam_score)  # Añadir df después de reemplazo de None por 0

    # Selección de columnas filtradas con SID
    df_homework_scores = df_final_exam_score.select(*homework_scores_columns, "SID")
    df_homework_max_points = df_final_exam_score.select(*homework_max_points_columns, "SID")
    
    # Suma puntajes de hw para cada estudiante utilizando reduce con operator.add
    df_homework_temp = df_homework_scores.withColumn(
        "SUM_HW_SCORES", 
        reduce(operator.add, [F.col(c) for c in homework_scores_columns])
    ).join(
        df_homework_max_points.withColumn(
            "SUM_HW_MAX", 
            reduce(operator.add, [F.col(c) for c in homework_max_points_columns])
        ), 
        on="SID", 
        how="inner"
    )
    lst_dfs.append(df_homework_temp)  # Añadir df después de la suma de hw

    # Calcula el puntaje total de cada hw como la proporción entre las sumas de puntajes y puntos máximos
    df_homework_temp = df_homework_temp.withColumn(
        "TOTAL_HOMEWORK",
        F.when(F.col("SUM_HW_MAX") == 0, 0).otherwise(F.col("SUM_HW_SCORES") / F.col("SUM_HW_MAX"))
    )
    # Si SUM_HW_MAX es 0, asigna 0 a TOTAL_HOMEWORK para evitar división por cero

    # Calcular el puntaje promedio del homework
    df_homework_temp = df_homework_temp.withColumn(
        "AVERAGE_HOMEWORK", 
        F.col("TOTAL_HOMEWORK") / F.lit(len(homework_scores_columns))
    )
    # Añade la columna de puntaje promedio de homework
    lst_dfs.append(df_homework_temp)  # Añadir df después de calcular el puntaje promedio

    # Selecciona la mayor puntuación entre el puntaje total y el promedio para cada estudiante
    df_homework_temp = df_homework_temp.withColumn(
        "HOMEWORK_SCORE",
        F.greatest(F.col("TOTAL_HOMEWORK"), F.col("AVERAGE_HOMEWORK"))
    )
    # Elige el valor máximo entre TOTAL_HOMEWORK y AVERAGE_HOMEWORK

    # Une el DataFrame de hws calculadas con el DataFrame original
    df_final_combined = df_final_exam_score.join(
        df_homework_temp.select("SID", "TOTAL_HOMEWORK", "AVERAGE_HOMEWORK", "HOMEWORK_SCORE"), 
        on="SID", 
        how="inner"
    )
    # Combina los cálculos de hw con el DataFrame original
    lst_dfs.append(df_final_combined)  # Añadir df final combinado para debugging

    dict_debug = {"dfs": lst_dfs, "params": lst_params} if debug else {}

    return df_final_combined, dict_debug


# In[20]:


df_final_combined = calculate_homework_scores(df_final_exam_score)[0]

# ## Calculating Quiz Score

# In[21]:


def calculate_quiz_scores(df_final_combined: DataFrame, debug=False) -> DataFrame:
    """
    Calculates quiz scores based on total and average scores.
    Then, determines the best score between the total and the average.
    
    :param df_final_combined: DataFrame containing quiz scores.
    :param debug: Boolean flag to enable debugging mode.
    :return: DataFrame with added columns TOTAL_QUIZZES, AVERAGE_QUIZ_SCORES, and QUIZ_SCORE.
    """
    
    lst_dfs = []
    lst_params = [df_final_combined]

    # Selección de las columnas de QUIZ para guardarlo en un DF aparte.
    ls_columns_quiz = df_final_combined.columns
    quiz_scores_columns = [col for col in ls_columns_quiz if re.match(r"^QUIZ_\d+_GRADES$", col)]
    df_quiz_score = df_final_combined.select(*quiz_scores_columns, "SID")
    lst_dfs.append(df_quiz_score)  # Añadir df_quiz_score a lst_dfs para debugging

    # Definición los puntos máximos para cada quiz en un DataFrame y un diccionario
    df_quiz_max_points = spark.createDataFrame([
        ("quiz_1_grades", 11), 
        ("quiz_2_grades", 15), 
        ("quiz_3_grades", 17), 
        ("quiz_4_grades", 14), 
        ("quiz_5_grades", 12)
    ], schema=["Quiz", "max_score"])

    dict_quiz_max_points = {
        "quiz_1_grades": 11, 
        "quiz_2_grades": 15, 
        "quiz_3_grades": 17, 
        "quiz_4_grades": 14, 
        "quiz_5_grades": 12
    }
    lst_dfs.append(df_quiz_max_points)  # Añadir df_quiz_max_points a lst_dfs para debugging

    # Verifica si hay columnas de quizzes para sumar
    if len([col for col in df_quiz_score.columns if col != "SID"]) > 0:
        # Calcula la suma de los puntajes de los quizzes
        sum_of_quiz_scores = df_quiz_score.withColumn(
            "SUM_QUIZ_SCORES", reduce(lambda a, b: a + b, [F.col(c) for c in df_quiz_score.columns if c != "SID"], F.lit(0))
        )
        lst_dfs.append(sum_of_quiz_scores)  # Añadir sum_of_quiz_scores a lst_dfs para debugging
    else:
        print("No hay columnas de quiz para sumar.")

    # Suma los puntos máximos de los quizzes
    sum_quiz_max_scores = df_quiz_max_points.agg(F.sum("max_score")).collect()[0][0]

    # Calcula el puntaje total de los quizzes
    sum_of_quiz_scores_temp = sum_of_quiz_scores.withColumn(
        "TOTAL_QUIZZES", (sum_of_quiz_scores["SUM_QUIZ_SCORES"] / sum_quiz_max_scores)
    )
    lst_dfs.append(sum_of_quiz_scores_temp)  # Añadir sum_of_quiz_scores_temp a lst_dfs para debugging

    # Calcula el puntaje promedio de los quizzes
    df_average_quiz_scores_expr = sum(
        (F.col(quiz) / F.lit(dict_quiz_max_points[quiz])) for quiz in dict_quiz_max_points.keys()
    )
    print(df_average_quiz_scores_expr)

    # Añade la columna de puntaje promedio de los quizzes
    sum_of_quiz_scores_temp = sum_of_quiz_scores_temp.withColumn(
        "AVERAGE_QUIZ_SCORES", (df_average_quiz_scores_expr / len(dict_quiz_max_points))
    )
    lst_dfs.append(sum_of_quiz_scores_temp)  # Añadir AVERAGE_QUIZ_SCORES a lst_dfs para debugging

    # Max() entre el puntaje total y el promedio para cada estudiante
    df_quiz_best_score = sum_of_quiz_scores_temp.withColumn(
        "QUIZ_SCORE", F.greatest(sum_of_quiz_scores_temp["TOTAL_QUIZZES"], sum_of_quiz_scores_temp["AVERAGE_QUIZ_SCORES"])
    )
    lst_dfs.append(df_quiz_best_score)  # Añadir df_quiz_best_score a lst_dfs para debugging

    # join() con los puntajes calculados a df_final_combined
    df_final_combined = df_final_combined.join(
        df_quiz_best_score.select("QUIZ_SCORE", "TOTAL_QUIZZES", "AVERAGE_QUIZ_SCORES", "SID"), 
        on="SID", how="inner"
    )
    lst_dfs.append(df_final_combined)  # Añadir df_final_combined a lst_dfs para debugging

    dict_debug = {"dfs": lst_dfs, "params": lst_params} if debug else {}

    return df_final_combined, dict_debug


# In[22]:


df_final_combined = calculate_quiz_scores(df_final_combined)[0]


# ## Calculate Letter Grade

# In[23]:


def calculate_letter_grade(df_final_combined: DataFrame, debug=False) -> DataFrame:
    """
    Calculates the final grade based on weighted scores from exams, quizzes, and homework,
    and assigns a letter grade (A-F) based on the final score.
    
    :param df_final_combined: DataFrame containing exam, quiz, and homework scores.
    :param debug: Boolean flag to enable debugging mode.
    :return: DataFrame with added columns FINAL_SCORE, CEILING_SCORE, and FINAL_GRADE.
    """

    lst_dfs = []
    lst_params = [df_final_combined]

    weightings_dict = {
        "EXAM_1_SCORE": 0.05,
        "EXAM_2_SCORE": 0.1,
        "EXAM_3_SCORE": 0.15,
        "QUIZ_SCORE": 0.30,
        "HOMEWORK_SCORE": 0.4
    }

    # Calcular el puntaje final basado en los pesos
    df_weighted_scores = df_final_combined.withColumn(
        "FINAL_SCORE",
        sum(F.col(col) * F.lit(weight) for col, weight in weightings_dict.items())
    )
    lst_dfs.append(df_weighted_scores)  # Añadir df_weighted_scores a lst_dfs para debugging

    # Calcular el puntaje final redondeado al entero superior
    df_weighted_scores = df_weighted_scores.withColumn(
        "CEILING_SCORE", F.ceil(F.col("FINAL_SCORE") * 100)
    )
    lst_dfs.append(df_weighted_scores)  # Añadir df_weighted_scores después del CEILING_SCORE

    # Función para asignar calificación en letra
    def assign_letter_grade(final_score):
        if final_score >= 90:
            return "A"
        elif final_score >= 80:
            return "B"
        elif final_score >= 70:
            return "C"
        elif final_score >= 60:
            return "D"
        else:
            return "F"

    grade_udf = F.udf(assign_letter_grade, StringType())

    # Asignar la calificación final
    df_weighted_scores = df_weighted_scores.withColumn(
        "FINAL_GRADE", grade_udf(F.col("CEILING_SCORE"))
    )
    lst_dfs.append(df_weighted_scores)  # Añadir df_weighted_scores después de FINAL_GRADE

    # Unir los resultados con df_final_combined
    df_final_combined = df_final_combined.join(
        df_weighted_scores.select("SID", "FINAL_SCORE", "CEILING_SCORE", "FINAL_GRADE"), 
        on="SID", 
        how="inner"
    )
    lst_dfs.append(df_final_combined)  # Añadir df_final_combined para debugging


    dict_debug = {"dfs": lst_dfs, "params": lst_params} if debug else {}

    return df_final_combined, dict_debug


# In[24]:


df_final_combined = calculate_letter_grade(df_final_combined)[0]



# In[26]:


df_final_combined.coalesce(1).write.csv("./final.csv", header=True, mode="overwrite")


# In[ ]:




