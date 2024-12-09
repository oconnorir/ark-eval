import os
import sys
import csv

# DATABASE CONNECTION
from db_connection import start_db_connection


def process_csv_directory(directory_path):
	# CONNECT TO DB
	connection = start_db_connection()
	cursor = connection.cursor()

	try:
		# Iterate over all files in the directory
		for file_name in os.listdir(directory_path):
			if file_name.endswith(".csv"):
				table_name = os.path.splitext(file_name.lower())[0].lower()  # Use the file name (without extension) as the table name
				# print(table_name)

				# Open the CSV file
				csv_file_path = os.path.join(directory_path, file_name)
				with open(csv_file_path, mode="r") as file:
					reader = csv.reader(file)

					# Extract the header row and convert column names to lowercase
					headers = next(reader)
					headers = [header.strip().lower() for header in headers]

					# Dynamically create the table if it doesn't exist
					columns_definition = ", ".join([f"{header} TEXT" for header in headers])
					create_table_query = f"""
						CREATE TABLE IF NOT EXISTS {table_name} ({columns_definition});
					"""
					cursor.execute(create_table_query)

					# Prepare the SQL insert query
					columns = ", ".join(headers)
					placeholders = ", ".join(["%s"] * len(headers))
					insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"

					# Insert each row from the CSV file
					for row in reader:
						cursor.execute(insert_query, row)
						print(row)

		connection.commit()
		print("Data inserted successfully.")
	except Exception as e:
		print(f"There was an error inserting data into the database: {e}")
	finally:
		cursor.close()
		connection.close()


# ADD PATH TO DIRECTORY WHEN CALLING THIS
# FUNCTION FROM THE COMMAND LINE
# EX: python3 create_tables_from_csv.py db_files/initial_data_files
file_path_argument = sys.argv[1]

# INSERT DATA INTO DATABASE TABLES
process_csv_directory(file_path_argument)
