import sys
import pandas as pd


def load_data(messages_filepath, categories_filepath):

    # Load csv data into table "raw_data".
    messages = pd.read_csv(messages_filepath)
    categories = pd.read_csv(categories_filepath)
    raw_data = messages.merge(categories, on='id')

    return raw_data


def clean_data(raw_data):

    # Make table "raw_categories" from raw_data column "categories".
    raw_categories = raw_data.categories.str.split(';', expand=True)

    # Make list "category_headers".
    raw_category_headers = raw_categories.iloc[0]  # use only one row (all rows contain same category list)
    category_headers = raw_category_headers.apply(lambda x: x[:-2]) # remove dash number suffixes

    # Add "category_headers" as header row to "raw_categories".
    raw_categories.columns = category_headers

    # Make table "clean_categories" by removing all but the integer suffixes from the data in "raw_categories".
    for column in raw_categories:
        raw_categories[column] = raw_categories[column].str[-1]
        raw_categories[column] = raw_categories[column].astype(int)
    clean_categories = raw_categories

    # Remove column "categories" from "raw_data".
    raw_data.drop(columns=['categories'], inplace=True)

    # Make table "clean_data" by column-wise-appending "clean_categories" to "raw_data".
    clean_data = raw_data.join(clean_categories)

    # Remove duplicate rows from "clean_data".
    clean_data.drop_duplicates(inplace=True)

    return clean_data


def save_data(df, database_filename):
    pass


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
