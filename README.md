# Script Meta-Fields Extractor

```plaintext
    __  ___     __           _______      __    __        ______     __                  __            
   /  |/  /__  / /_____ _   / ____(_)__  / /___/ /____   / ____/  __/ /__________ ______/ /_____  _____
  / /|_/ / _ \/ __/ __ `/  / /_  / / _ \/ / __  / ___/  / __/ | |/_/ __/ ___/ __ `/ ___/ __/ __ \/ ___/
 / /  / /  __/ /_/ /_/ /  / __/ / /  __/ / /_/ (__  )  / /____>  </ /_/ /  / /_/ / /__/ /_/ /_/ / /    
/_/  /_/\___/\__/\__,_/  /_/   /_/\___/_/\__,_/____/  /_____/_/|_|\__/_/   \__,_/\___/\__/\____/_/     
```

## Table of Contents
- [Description](#description)
- [Getting Started](#getting-started)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgements](#acknowledgements)

---

## Description
The **Script Meta-Fields Extractor** is a Python-based tool designed to extract metadata (field names, data types, and example values) from a variety of file formats, including:
- CSV
- Excel (`.xls`, `.xlsx`)
- JSON
- XML
- Parquet
- QVD (with the version 1.1)

The tool processes data files located in the `inputs` directory and generates metadata reports saved in the `outputs` directory.

---

## Getting Started

### Prerequisites
Ensure you have the following installed:
1. **Python 3.6+**
2. **pip (Python package manager)**

### Installation
1. Clone this repository:
   ```bash
   git clone <repository_url>
   cd script-meta-fields-extractor
   ```

2. Install the required Python libraries:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up the folder structure:
   - Ensure the `inputs` folder exists and place your data files inside it.
   - The script will automatically create the `outputs` folder if it doesn't exist.

---

## Usage
1. Place your data files (e.g., `sample.csv`, `data.json`) in the `inputs` folder.
2. Run the script:
   ```bash
   python data_info_extractor.py
   ```
3. Follow the prompts to select a file for analysis.
4. The tool will display the metadata (field names, types, examples) in the terminal and save the output to the `outputs` directory.

---

## Project Structure

```plaintext
SCRIPT-META-FIELDS-EXTRACTOR/
├── inputs/                 # Input folder containing data files (CSV, JSON, etc.)
│   ├── sample.csv
│   ├── sample.json
│   ├── sample.parquet
│   ├── sample.xls
│   └── sample.xml
├── outputs/                # Output folder for processed metadata reports
│   ├── .gitignore          # Ignores unnecessary files
├── data_info_extractor.py  # Main Python script for metadata extraction
├── LICENSE                 # License information
├── README.md               # Project documentation
├── requirements.txt        # Python dependencies
```

---

## Contributing
We welcome contributions to improve this project! To contribute:
1. Fork the repository.
2. Create a new branch (`feature/your-feature-name`).
3. Commit your changes with clear and concise messages.
4. Push to your branch and open a pull request.

---

## License
This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.

---

## Acknowledgements
- OpenAI's **o1** for assisting with the refactoring and creating this README. 