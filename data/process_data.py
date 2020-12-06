import sys
import pandas as pd
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):

    # Load the raw csv data into one table.
    messages = pd.read_csv(messages_filepath)
    categories = pd.read_csv(categories_filepath)
    raw_data_table = messages.merge(categories, on='id')

    return raw_data_table


def clean_data(raw_data_table):

    cleaned_category_table = clean_categories(raw_data_table)

    # Remove the "categories" column from the raw data table.
    raw_data_table.drop(columns=['categories'], inplace=True)

    # Make a cleaned data table by column-wise-appending the cleaned category table to the raw data table.
    cleaned_data_table = raw_data_table.join(cleaned_category_table)

    # Remove duplicate rows from the cleaned data table.
    cleaned_data_table.drop_duplicates(inplace=True)

    return cleaned_data_table


def clean_categories(raw_data_table):

    # Make a raw category table from the "categories" column in the raw data table.
    raw_category_table = raw_data_table.categories.str.split(';', expand=True)

    # Make a cleaned category list.
    raw_category_list = raw_category_table.iloc[0]  # use only one row (all rows contain same category list)
    cleaned_category_list = raw_category_list.apply(lambda x: x[:-2]) # remove dash number suffix from eacn category

    # Add the cleaned category list as the header row to the raw category table.
    raw_category_table.columns = cleaned_category_list

    # Make a cleaned category table by removing all but the integer suffixes from the data in the raw category table.
    for column in raw_category_table:
        raw_category_table[column] = raw_category_table[column].str[-1]
        raw_category_table[column] = raw_category_table[column].astype(int)
    cleaned_category_table = raw_category_table

    return cleaned_category_table


def save_data(data_table, database_filename):

    # Make database engine.
    engine = create_engine('sqlite:///' + database_filename)

    # Make database file.
    data_table.to_sql('messages', engine, if_exists='replace', index=False)


def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        raw_data = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        cleaned_data = clean_data(raw_data)

        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(cleaned_data, database_filepath)

        print('Cleaned data saved to database!')

    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()
