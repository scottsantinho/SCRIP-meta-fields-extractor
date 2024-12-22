import pandas as pd
import pandas.api.types as ptypes
import random
import sys
import os
import json


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

def infer_data_type_for_series(series):
    """
    Infer the data type for a pandas Series by checking its dtype.
    """
    if ptypes.is_datetime64_any_dtype(series):
        return 'Datetime'
    elif ptypes.is_integer_dtype(series):
        return 'Integer'
    elif ptypes.is_float_dtype(series):
        return 'Float'
    elif ptypes.is_bool_dtype(series):
        return 'Boolean'
    elif ptypes.is_object_dtype(series):
        # Might be all text or a mixture of text and other values
        return 'String'
    else:
        return 'Unknown'
    
def infer_data_type(value):
    if isinstance(value, bool):
        return 'Boolean'
    elif isinstance(value, int):
        return 'Integer'
    elif isinstance(value, float):
        return 'Float'
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
        dtype = infer_data_type_for_series(df[column])
        example = get_random_example_tabular(df[column])
        output_lines.append(f"{column};{dtype};{example}")
    return output_lines

def get_random_example_value(values):
    """Get a random example from a list of values."""
    non_null_values = [v for v in values if v is not None]
    if not non_null_values:
        return 'N/A'
    return random.choice(non_null_values)

def process_semi_structured(data, parent_key=''):
    """Recursively process semi-structured data and collect field info."""
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

def process_semi_structured_file(file_path):
    """Process a semi-structured JSON file and collect field info."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Error reading JSON file: {e}")
        sys.exit(1)
    
    field_info = []
    if isinstance(data, list):
        # To avoid duplicates, collect unique field paths with examples
        seen_fields = {}
        for item in data:
            temp_info = process_semi_structured(item)
            for line in temp_info:
                field, dtype, example = line.split(';', 2)
                if field not in seen_fields:
                    seen_fields[field] = (dtype, example)
        for field, (dtype, example) in seen_fields.items():
            field_info.append(f"{field};{dtype};{example}")
    else:
        field_info = process_semi_structured(data)
    
    return field_info

def read_file(file_path):
    """Read the file based on its extension and return a DataFrame or a JSON path."""
    _, file_extension = os.path.splitext(file_path)
    file_extension = file_extension.lower()
    
    try:
        if file_extension == '.csv':
            df = pd.read_csv(file_path)
            return 'tabular', df
        elif file_extension in ['.xls', '.xlsx']:
            df = pd.read_excel(file_path, engine='openpyxl' if file_extension == '.xlsx' else 'xlrd')
            return 'tabular', df
        elif file_extension == '.json':
            return 'semi_structured', file_path
        elif file_extension == '.xml':
            df = pd.read_xml(file_path)
            return 'tabular', df
        elif file_extension == '.parquet':
            df = pd.read_parquet(file_path)
            return 'tabular', df
        else:
            print(f"❌ Unsupported file format: {file_extension}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        sys.exit(1)

def write_output(output_lines, output_file):
    """Write the collected output lines to a file."""
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            for line in output_lines:
                f.write(line + '\n')
    except Exception as e:
        print(f"❌ Error writing to file: {e}")
        sys.exit(1)

def analyze_file(file_name):
    """
    Main logic for reading, processing, and writing the output of a chosen file.
    """
    # Build full path from 'inputs' folder
    file_path = os.path.join('inputs', file_name)
    
    # 1. Read the file
    file_type, content = read_file(file_path)
    
    # 2. Process the file
    if file_type == 'tabular':
        df = content
        output_lines = process_tabular(df)
    elif file_type == 'semi_structured':
        output_lines = process_semi_structured_file(content)
    else:
        print("❌ Unsupported file type.")
        sys.exit(1)
    
    # 3. Print to terminal
    print("✅ Fields detected:")
    for line in output_lines:
        print(line)
    
    # 4. Write to 'outputs' folder
    # e.g., outputs/extracted_data.csv -> extracted_data.csv
    base_name = os.path.splitext(file_name)[0]
    output_path = os.path.join('outputs', f"extracted_{base_name}.txt")
    write_output(output_lines, output_path)
    print(f"✅ Output saved to: {output_path}")

def main():
    """
    Displays ASCII art, lists files in 'inputs', 
    and lets you choose which file to analyze by index.
    """
    while True:
        # Display ASCII art
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
        
        # Prompt for file selection
        choice = input("Please enter the file number or 'q' to quit: ").strip()
        
        if choice.lower() == 'q':
            print("✅ Goodbye ! 👋")
            break
        
        # Validate choice
        if not choice.isdigit() or not (1 <= int(choice) <= len(files)):
            print("❌ Invalid choice. Please try again.")
            continue
        
        # Analyze selected file
        selected_index = int(choice) - 1
        selected_file = files[selected_index]
        analyze_file(selected_file)
        
        # Ask if we want to analyze another file or quit
        repeat = input("Do you want to analyze another file? (y/n): ").strip().lower()
        if repeat != 'y':
            print("✅ Goodbye ! 👋")
            break

if __name__ == "__main__":
    main()