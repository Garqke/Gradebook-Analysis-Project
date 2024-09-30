# README Gradebook Analysis Project

## Project Overview

This project is a Python-based data analysis tool designed to load, transform, and analyze student grade data using **PySpark**. It includes functions for handling CSV files that store homework, quiz, and exam grades, and combines this information to generate aggregated views and insights on students' performance. Additionally, the project features unit tests to ensure the correctness of the key functionalities using the `unittest` module.

## Folder Structure

- **`gradebook.py`**: The main module containing all the functions to load, transform, and process CSV files. It includes functions to handle homework grades, quiz grades, and a general function to load and format any CSV data with specified transformations.
  
- **`TestGradeBook.py`**: A Python unit test file for validating the functionality of the methods defined in `gradebook.py`. It uses mocks to simulate CSV file loading and checks the correctness of the transformations.

- **`visualization.ipynb`**: A Jupyter notebook that contains visualizations and further analysis of the grade data. It leverages PySpark for data handling and includes steps for loading the data and generating plots to explore student performance visually.

- **`gradebook.ipynb`**: Another Jupyter notebook that provides an interactive environment to explore and test the main functions from `gradebook.py`. This notebook allows you to see how the data is loaded, transformed, and manipulated step-by-step.

## Key Features

### Functions in `gradebook.py`

1. **`load_and_format_csv(file_path: str, transformations: dict) -> DataFrame`**:
   - Loads a CSV file and applies transformations to its columns based on the provided dictionary. Each transformation is specified as a PySpark column expression.
   - Example transformation: converting a column to uppercase or applying arithmetic operations on numeric columns.

2. **`load_hw_grades_csv(file_path: str) -> DataFrame`**:
   - Loads and formats a CSV containing homework and exam grades, renaming columns and filtering out unnecessary columns (e.g., submission dates).

3. **`load_quiz_grades(data_folder: str) -> DataFrame`**:
   - Combines multiple quiz grade CSV files from a specified folder into a single DataFrame. It ensures that each quiz file is processed correctly and the results are merged by student identifiers.

### Unit Tests in `TestGradeBook.py`

- The unit test suite uses the `unittest` framework and applies mocking to simulate file reading operations with `PySpark`. Each function in `gradebook.py` is thoroughly tested, ensuring correct behavior with various transformations and data loading tasks.
- **Tested functions** include:
  - `load_and_format_csv`
  - `load_hw_grades_csv`
  - `load_quiz_grades`

### Jupyter Notebooks

- **`visualization.ipynb`**: 
  - This notebook focuses on visualizing the processed data, showing insights such as student performance trends, quiz comparisons, and homework completion rates.
  
- **`gradebook.ipynb`**: 
  - This notebook offers an interactive way to test and run the main data loading and processing functions from the `gradebook.py` script, allowing step-by-step execution and analysis of the data.

## How to Run

### Requirements

- **Python 3.x**
- **PySpark**: The project uses PySpark for distributed data processing. Ensure that you have PySpark installed in your environment.
- **Jupyter**: To run the `.ipynb` notebooks, Jupyter should be installed.

### Installation

1. Clone the repository or download the files.
   
2. Create a virtual environment (optional but recommended):

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. Install the required dependencies:

   ```bash
   pip install -r requirements.txt
   ```

### Running Tests

To run the unit tests, use:

```bash
python -m unittest discover -s tests
```

This will discover and execute all the unit tests in the `tests` folder, ensuring that the functions behave as expected.

### Running the Jupyter Notebooks

Start Jupyter and open the notebooks:

```bash
jupyter notebook
```

- Open `gradebook.ipynb` to explore the main functionalities.
- Open `visualization.ipynb` to generate data visualizations and insights based on the student grade data.

## Diagrams

### Table Structure
**![Table Structure](<./images/Project1-Tables.drawio.png>)**


### Data Flow
**![Data Flow](./images/Project1-Diagrama%20de%20Flujos.drawio.png)**