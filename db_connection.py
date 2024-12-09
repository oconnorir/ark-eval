import psycopg2

# WHILE I AM COMMITTING THESE VALUES, IN A 'REAL WORLD' SCENARIO, THEY WOULD NEVER BE COMMITTED TO A REMOTE REPOSITORY. THEY WOULD INSTEAD BE STORED IN A MORE SECURE WAY SUCH AS SECRETS MANAGER OR SOMETHING SIMILAR.
# I AM DOING SO NOW ONLY FOR THE SAKE OF COVERAGE FOR THIS PROJECT.
database_config = {
	"dbname": "arkatechture",
	"user": "aaronoconnor01",
	"password": '',
	"host": "localhost",
	"port": 5436
}


def start_db_connection():
	try:
		connection = psycopg2.connect(**database_config)
		return connection
	except Exception as ex:
		print(f"There was an error connecting to the database - {ex}")
		raise
