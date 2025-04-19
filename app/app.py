from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

CASSANDRA_KEYSPACE = "storage"

if  __name__ == '__main__':
	cluster = Cluster(['cassandra-server'])
	session = cluster.connect()

	keyspace_query = SimpleStatement(f"""
		CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
		WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
	""")

	#Drop old tables
	rows = session.execute(f"""
		SELECT table_name FROM system_schema.tables
		WHERE keyspace_name='{CASSANDRA_KEYSPACE}';
	""")
	tables = [row.table_name for row in rows]
	for table in tables:
		drop_query = f"DROP TABLE IF EXISTS {CASSANDRA_KEYSPACE}.{table};"
		print(f"Dropping table: {table}")
		session.execute(drop_query)

	session.execute(keyspace_query)
	session.set_keyspace(CASSANDRA_KEYSPACE)
	print(f"Switched to keyspace: {CASSANDRA_KEYSPACE}")

	session.execute("""
			CREATE TABLE IF NOT EXISTS inv_index (
				term text,
				doc_id int,
				term_frequency int,
				PRIMARY KEY ((term), doc_id)
			) WITH CLUSTERING ORDER BY (doc_id ASC);
		""")
	print("Table 'inv_index' created")

	session.execute("""
			CREATE TABLE IF NOT EXISTS doc_stats (
				doc_id int PRIMARY KEY,
				doc_length int,
			);
		""")
	print("Table 'doc_stats' created")

	session.execute("""
			CREATE TABLE IF NOT EXISTS vocab_stats (
				term text PRIMARY KEY,
				doc_frequency int
			);
		""")
	print("Table 'vocab_stats' created")
