########################################
# Imports
########################################

# Library for handling tabular data
import pandas as pd
import pandas.api.types as ptypes

# Library for parsing XML
import xmltodict

# Standard libraries
import random
import sys
import os
import json
import re
from datetime import datetime

# QVD reader (install via: pip install qvd)
try:
    from qvd import qvd_reader
except ImportError:
    qvd_reader = None


########################################
# ASCII Art
########################################

def display_ascii_art():
    """
    Displays ASCII art.
    """
    print(r"""
    __  ___     __           _______      __    __        ______     __                  __            
   /  |/  /__  / /_____ _   / ____(_)__  / /___/ /____   / ____/  __/ /__________ ______/ /_____  _____
  / /|_/ / _ \/ __/ __ `/  / /_  / / _ \/ / __  / ___/  / __/ | |/_/ __/ ___/ __ `/ ___/ __/ __ \/ ___/
 / /  / /  __/ /_/ /_/ /  / __/ / /  __/ / /_/ (__  )  / /____>  </ /_/ /  / /_/ / /__/ /_/ /_/ / /    
/_/  /_/\___/\__/\__,_/  /_/   /_/\___/_/\__,_/____/  /_____/_/|_|\__/_/   \__,_/\___/\__/\____/_/     
                                                                                                       
    """)


########################################
# Data Type Inference Logic
########################################

def is_numeric_series(series):
    """
    Check if all non-null values in the Series can be converted to float.
    """
    try:
        # Convert to float and drop any resulting NaNs
        pd.to_numeric(series.dropna(), errors='raise')
        return True
    except ValueError:
        return False

def is_date_series(series, date_formats=None):
    """
    Check if all non-null values in the Series conform to recognizable date/time formats.
    You can expand `date_formats` as needed (e.g. ['%Y-%m-%d', '%m/%d/%Y']).
    """
    if not date_formats:
        date_formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y']
    
    # Check each value with possible formats
    def can_parse_date(value):
        for fmt in date_formats:
            try:
                datetime.strptime(value.strip(), fmt)
                return True
            except (ValueError, TypeError):
                pass
        return False

    # We only test if all non-null are date-like
    non_nulls = series.dropna().astype(str)
    return all(can_parse_date(val) for val in non_nulls)

def is_boolean_series(series):
    """
    Check if all non-null values are either "true/false", "True/False", or "0/1".
    """
    bool_values = {'true', 'false', '0', '1'}
    non_nulls = series.dropna().astype(str).str.lower().str.strip()
    return all(val in bool_values for val in non_nulls)

def apply_qlik_cloud_inference(df):
    """
    Mimic Qlik Cloud-like logic:
      1) If boolean pattern -> Boolean
      2) Else if numeric -> Numeric
      3) Else if date/time -> Date/Time
      4) Else -> String/Text
      5) Dual -> If numeric values can also be recognized as date/time
                 we store them in a special "dual" approach 
                 (both a numeric representation & a string representation).
    """
    # We'll store 'Dual' as a second column for demonstration, 
    # in real Qlik it's more seamless.
    for col in df.columns:
        # Only analyze object/string columns to see if they can be cast
        if ptypes.is_object_dtype(df[col]):
            # Step 1: Is Boolean?
            if is_boolean_series(df[col]):
                # Convert to True/False
                df[col] = df[col].apply(lambda x: str(x).lower() in ['true', '1'])
            # Step 2: Is Numeric?
            elif is_numeric_series(df[col]):
                numeric_values = pd.to_numeric(df[col], errors='coerce')
                
                # Step 3: Also check if numeric can be date/time => Dual
                # For example, Qlik stores date as integer offset from Dec 30, 1899
                # We'll do a simplified check: if these numeric values can be in a 
                # plausible date range we treat them as Dual. 
                # This is a big assumption: you can tune it to your actual data!
                # Alternatively, we can check if the string representation is date-like.
                
                # Quick approach: if is_date_series -> let's do "dual"
                if is_date_series(df[col]):  
                    # "Dual" approach in Qlik is a single field with both numeric & textual representation
                    # We'll mimic by storing numeric in the column and textual in a second col for demonstration
                    df[col + "_DualText"] = df[col].copy()
                    df[col] = numeric_values
                else:
                    # Just numeric
                    df[col] = numeric_values
            # Step 4: If not numeric or boolean, is it date/time?
            elif is_date_series(df[col]):
                df[col] = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
            else:
                # Step 5: remains string
                pass
        # If already numeric or datetime, skip
    return df


########################################
# Helper Functions
########################################

def infer_data_type_for_series(series):
    """
    A bridging function that checks the Series dtype 
    after our Qlik-likes conversions.
    """
    if ptypes.is_bool_dtype(series):
        return 'Boolean'
    elif ptypes.is_integer_dtype(series) or ptypes.is_float_dtype(series):
        return 'Numeric'
    elif ptypes.is_datetime64_any_dtype(series):
        return 'Date/Time'
    elif ptypes.is_object_dtype(series):
        # If we see that there's a paired 'DualText' column, let's call this 'Dual'
        return 'String'
    else:
        return 'Unknown'

def infer_data_type(value):
    """
    Infer the data type of a single value at a Python level for semi-structured data.
    """
    # Qlik doesn't often separate list/dict in normal usage, 
    # but let's keep your original approach.
    if isinstance(value, bool):
        return 'Boolean'
    elif isinstance(value, (int, float)):
        return 'Numeric'
    elif isinstance(value, str):
        return 'String'
    elif isinstance(value, list):
        return 'List'
    elif isinstance(value, dict):
        return 'Object'
    else:
        return 'Unknown'

def get_random_example_tabular(series):
    """
    Get a random non-null example from a pandas Series.
    """
    non_null_series = series.dropna()
    if non_null_series.empty:
        return 'N/A'
    return random.choice(non_null_series.tolist())

def process_tabular(df):
    """
    Process tabular DataFrame and collect field info.
    """
    output_lines = []
    for column in df.columns:
        # Convert the column to a string for checking suffix
        column_str = str(column)
        
        dtype = infer_data_type_for_series(df[column])
        example = get_random_example_tabular(df[column])
        
        if column_str.endswith("_DualText"):
            # We'll skip printing this as a separate field or show it differently
            output_lines.append(f"{column_str} (Dual string representation);String;{example}")
        else:
            # Use the string version as part of the output so we don't break the rest
            output_lines.append(f"{column_str};{dtype};{example}")
    
    return output_lines

def process_semi_structured(data, parent_key=''):
    """
    Recursively process semi-structured data (dict or list) 
    and collect field info in a list of 'field;type;example' strings.
    """
    lines = []
    if isinstance(data, dict):
        for key, value in data.items():
            full_key = f"{parent_key}.{key}" if parent_key else key
            lines.extend(process_semi_structured(value, full_key))
    elif isinstance(data, list):
        for index, item in enumerate(data):
            full_key = f"{parent_key}[{index}]"
            lines.extend(process_semi_structured(item, full_key))
    else:
        data_type = infer_data_type(data)
        example = data if data is not None else 'N/A'
        lines.append(f"{parent_key};{data_type};{example}")
    return lines

def process_semi_structured_data(data_or_path):
    """
    Unified function to handle both JSON (path) and XML (dict).
    Returns a list of 'field;type;example' lines.
    """
    data = None

    # If a string was passed, assume it's a JSON file path
    if isinstance(data_or_path, str) and os.path.isfile(data_or_path):
        try:
            with open(data_or_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"❌ Error reading JSON file: {e}")
            return []
    else:
        # Otherwise, assume it's already a dict (e.g. from xmltodict.parse())
        data = data_or_path

    # If it's a list at the top level, we want to avoid duplicates
    if isinstance(data, list):
        seen_fields = {}
        for item in data:
            temp_info = process_semi_structured(item)
            for line in temp_info:
                field, dtype, example = line.split(';', 2)
                if field not in seen_fields:
                    seen_fields[field] = (dtype, example)
        # Rebuild unique lines
        return [f"{field};{dtype};{example}" for field, (dtype, example) in seen_fields.items()]
    else:
        return process_semi_structured(data)


def write_output(output_lines, output_file):
    """
    Write the collected output lines to a file.
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            for line in output_lines:
                f.write(line + '\n')
    except Exception as e:
        print(f"❌ Error writing to file: {e}")


########################################
# Core File Reading & Analysis
########################################

def read_file(file_path):
    """
    Read the file based on its extension and return ('tabular' or 'semi_structured', content).
    """
    _, file_extension = os.path.splitext(file_path)
    file_extension = file_extension.lower()

    try:
        if file_extension == '.csv':
            df = pd.read_csv(file_path)
            return 'tabular', df
        elif file_extension in ['.xls', '.xlsx']:
            df = pd.read_excel(
                file_path,
                engine='openpyxl' if file_extension == '.xlsx' else 'xlrd'
            )
            return 'tabular', df
        elif file_extension == '.json':
            # We'll process JSON later in a unified function
            return 'semi_structured', file_path
        elif file_extension == '.xml':
            with open(file_path, 'r', encoding='utf-8') as f:
                xml_data = f.read()
            data = xmltodict.parse(xml_data)
            return 'semi_structured', data
        elif file_extension == '.parquet':
            df = pd.read_parquet(file_path)
            return 'tabular', df
        elif file_extension == '.qvd':
            # Check if qvd_reader is available
            if qvd_reader is None:
                print("❌ 'qvd' library is not installed. Please run 'pip install qvd'.")
                return None, None

            # Use qvd library to read the file into a pandas DataFrame
            df = qvd_reader.read(file_path)

            # 🔥 APPLY Qlik Cloud-like data type inference 🔥
            df = apply_qlik_cloud_inference(df)
            
            return 'tabular', df
        else:
            print(f"❌ Unsupported file format: {file_extension}")
            return None, None
    except Exception as e:
        print(f"❌ Error reading file '{file_path}': {e}")
        return None, None


def analyze_file(file_name):
    """
    Main logic for reading, processing, and writing the output of a chosen file.
    """
    file_path = os.path.join('inputs', file_name)
    
    file_type, content = read_file(file_path)

    if file_type is None:
        print("❌ Could not process file. Please check format or errors.")
        return

    if file_type == 'tabular':
        df = content
        output_lines = process_tabular(df)
    elif file_type == 'semi_structured':
        output_lines = process_semi_structured_data(content)
    else:
        print("❌ Unsupported file type.")
        return

    # 3. Print to terminal
    print("✅ Fields detected:")
    for line in output_lines:
        print(line)

    # 4. Write to 'outputs' folder
    base_name = os.path.splitext(file_name)[0]
    output_path = os.path.join('outputs', f"extracted_{base_name}.txt")
    write_output(output_lines, output_path)
    print(f"✅ Output saved to: {output_path}")


########################################
# Entry Point
########################################

def main():
    """
    Displays ASCII art, lists files in 'inputs', 
    and lets you choose which file to analyze by index.
    """
    while True:
        display_ascii_art()
        
        # List files in 'inputs'
        files = os.listdir('inputs')
        files = [f for f in files if not f.startswith('.')]  # Exclude hidden files
        
        if not files:
            print("⚠️ No files found in 'inputs' folder.")
            return
        
        print("✅ Files available for analysis:")
        for idx, file_name in enumerate(files, start=1):
            print(f"{idx}. {file_name}")
        
        choice = input("Please enter the file number or 'q' to quit: ").strip()
        
        if choice.lower() == 'q':
            print("✅ Goodbye ! 👋")
            break
        
        if not choice.isdigit() or not (1 <= int(choice) <= len(files)):
            print("❌ Invalid choice. Please try again.")
            continue
        
        selected_index = int(choice) - 1
        selected_file = files[selected_index]
        analyze_file(selected_file)
        
        repeat = input("Do you want to analyze another file? (y/n): ").strip().lower()
        if repeat != 'y':
            print("✅ Goodbye ! 👋")
            break


if __name__ == "__main__":
    main()