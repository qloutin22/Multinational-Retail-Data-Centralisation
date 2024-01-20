# Multinational-Retail-Data-Centralisation

<h1> Retail Data Centralization / Data Extraction 1</h1>

# Table of Contents

1. [Description](#description)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Usage](#usage)
5. [File Structure](#file-structure)
6. [License](#license)


## Description

The Retail Data Centralisation/Data Extraction project aims to centralise and streamline the extraction of retail data from various sources. It provides a robust system to gather data from multiple retail outlets, analyse it, and present insights for informed decision-making. The primary objectives include:

Developing an efficient data extraction mechanism from diverse retail systems.
Centralising data to a unified database or data warehouse for easy access and analysis.
Implementing tools for data cleansing, transformation, and visualization.
Enabling stakeholders to derive actionable insights for strategic decision-making.
Throughout the project, several key learnings were attained, including data integration techniques, ETL (Extract, Transform, Load) processes, database management, data cleaning SQL and Python. Understanding retail-specific data challenges and developing scalable solutions were integral parts of the learning process.

## Installation
To install and set up the Retail Data Centralization/Data Extraction project, follow these steps:

Clone the repository:

git clone https://github.com/qloutin22/Multinational-Retail-Data-Centralisation

## Install dependencies:

Navigate to the project directory and install necessary dependencies.

pip install -r requirements.txt
import tabula as ta
import pandas as pd
import yaml
from yaml.loader import SafeLoader
from sqlalchemy import create_engine,MetaData,Table
from database_utils import DatabaseConnector as dc
import requests
import boto3
from io import StringIO
import re
import yaml
import numpy as np


## Usage
To utilize the Retail Data Centralization/Data Extraction project effectively, follow these guidelines:

Data Extraction:

Run the extraction scripts or modules to collect data from retail sources.
Ensure the data extraction process runs smoothly and data is ingested into the system.
Data Centralization:

Verify that extracted data is centralized into the designated database or data warehouse.
Monitor data consistency and perform necessary transformations for normalization.
Data Analysis and Visualization:

Utilise provided tools or scripts to analyze and visualize retail data.
Generate reports or dashboards to showcase insights derived from the data.

## Project Breakdown

<h4>gitignore</h4>
<h4>data_cleaning.py</h4>
<h4>data_extraction.py</h4>
<h4>database_utils.py</h4>
<h4>db_creds.yaml</h4>
<h4>de_creds.yaml</h4>
<h4>upload_creds.yaml</h4>


## File Structure
The project's file structure is organized as follows:

#####
arduino
Copy code
retail-data-centralization/
│
├── data_cleaning.py
│
├── data_extraction.py
│
├── data_utils.py
│ 
├── visualization/
│
├── db_creds.yaml
|
|──de_creds.yaml
|
|──README.md
|
|──upload_creds.yaml


## License
This project is licensed under the MIT License - see the LICENSE file for details.