import pandas as pd
import sys
import logging

# Set up the logger
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def load_data(data_path):
    """Load data from a CSV file into a DataFrame."""
    return pd.read_csv(data_path)

def filter_data(df, countries):
    """Filter data based on the country."""
    return df[df['country'].isin(countries)]

def remove_columns(df, cols_to_remove):
    """Remove specified columns from the DataFrame."""
    return df.drop(columns=cols_to_remove)

def rename_columns(df, new_column_names):
    """Rename DataFrame columns."""
    return df.rename(columns=new_column_names)

def join_data(df1, df2, join_on):
    """Join two DataFrames on a specified column."""
    return pd.merge(df1, df2, on=join_on)

def save_data(df, path):
    """Save DataFrame to a CSV file."""
    df.to_csv(f"{path}/output.csv", index=False)

def main(client_data_path, financial_data_path, countries, output_path):
    """Main function to load, process and save data."""

    # Load data
    logger.info('Loading data...')
    client_data = load_data(client_data_path)
    financial_data = load_data(financial_data_path)

    # Filter data
    logger.info('Filtering data...')
    client_data = filter_data(client_data, countries)

    # Remove personal identifiable information
    logger.info('Removing personal identifiable information...')
    client_data = remove_columns(client_data, ['first_name', 'last_name'])
    financial_data = remove_columns(financial_data, ['cc_n'])

    # Rename columns
    logger.info('Renaming columns...')
    new_column_names = {'id': 'client_identifier', 'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type'}
    client_data = rename_columns(client_data, new_column_names)
    financial_data = rename_columns(financial_data, new_column_names)

    # Join data
    logger.info('Joining data...')
    final_data = join_data(client_data, financial_data, 'client_identifier')

    # Save data
    logger.info('Saving data...')
    save_data(final_data, output_path)

    logger.info('Data processing complete.')

if __name__ == '__main__':
    if len(sys.argv) != 5:
        print('Usage: script.py <client_data_path> <financial_data_path> <countries> <output_path>')
        sys.exit(-1)

    # Convert the comma-separated string of countries into a list
    countries = sys.argv[3].split(',')

    main(sys.argv[1], sys.argv[2], countries, sys.argv[4])
