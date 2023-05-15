# KommatiPara Client Data Processing

This project processes client and financial data of KommatiPara's Bitcoin trading company. The script filters out clients based on their country, removes personal identifiable information, renames certain columns for readability, joins the datasets on client ID, and saves the processed data.

## Setup

1. Clone this repository.
2. Install PySpark and its dependencies using pip:

`pip install pyspark`

## Usage

Run the script using the command:

`python script.py <client_data_path> <financial_data_path> <countries> <output_path>`

For example:

`python script.py ./dataset_one.csv ./dataset_two.csv "United Kingdom,Netherlands" ./client_data`

This will process the data and save the output in the `client_data` directory.

## Usage without PySpark

PySpark requires certain dependencies or Docker (which does not work well with Windows 11 Home, I think). So I created a Pandas version of the script instead.

Run the script using the command:

`python script_no_pyspark.py <client_data_path> <financial_data_path> <countries> <output_path>`

For example:

`python script_no_pyspark.py ./dataset_one.csv ./dataset_two.csv "United Kingdom,Netherlands" ./client_data`

This will process the data and save the output in the `client_data` directory.

## Testing

Testing can be done using the Chispa library. Install it using pip:

`pip install chispa`

Then, write test cases to compare actual and expected DataFrames.