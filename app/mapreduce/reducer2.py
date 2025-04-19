import sys
from collections import defaultdict

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement


def flush_batches(force=False):
	global batch_counts
	if force or batch_counts['inv'] >= batch_size:
		session.execute(batch_inv_idx)
		batch_inv_idx.clear()
		batch_counts['inv'] = 0

	if force or batch_counts['doc'] >= batch_size:
		session.execute(batch_doc_stats)
		batch_doc_stats.clear()
		batch_counts['doc'] = 0

	if force or batch_counts['vocab'] >= batch_size:
		session.execute(batch_vocab)
		batch_vocab.clear()
		batch_counts['vocab'] = 0


def process_key_data(key, data):
	global total_docs, total_length, batch_counts

	if key.startswith("DOC_"):
		print("Here")
		doc_id = int(key.split("_")[-1])
		doc_length = None
		is_valid_doc = False

		for value_type, value in data:
			doc_length = int(value)

		batch_doc_stats.add(doc_stats_stmt, (doc_id, doc_length))
		batch_counts['doc'] += 1
		print("Put into table")
	else:
		term = key
		doc_frequency = 0
		term_doc_pairs = []

		for value_type, value in data:
			if value_type == "DF":
				doc_frequency += int(value)
				continue

			doc_id = int(value_type)
			tf = int(value)
			term_doc_pairs.append((doc_id, tf))

		if doc_frequency > 0:
			batch_vocab.add(vocab_stats_stmt, (term, doc_frequency))
			batch_counts['vocab'] += 1

		for doc_id, tf in term_doc_pairs:
			batch_inv_idx.add(inverted_idx_stmt, (term, doc_id, tf))
			batch_counts['inv'] += 1

	flush_batches()


if __name__ == "__main__":
	cluster = Cluster(['cassandra-server'])
	session = cluster.connect()
	session.set_keyspace("storage")

	inverted_idx_stmt = session.prepare(
		"INSERT INTO inv_index (term, doc_id, term_frequency) VALUES (?, ?, ?)"
	)
	doc_stats_stmt = session.prepare(
		"INSERT INTO doc_stats (doc_id, doc_length) VALUES (?, ?)"
	)
	vocab_stats_stmt = session.prepare(
		"INSERT INTO vocab_stats (term, doc_frequency) VALUES (?, ?)"
	)

	batch_inv_idx = BatchStatement()
	batch_doc_stats = BatchStatement()
	batch_vocab = BatchStatement()
	batch_size = 100
	batch_counts = {'inv': 0, 'doc': 0, 'vocab': 0}

	total_docs = 0
	total_length = 0

	current_key = None
	key_data = defaultdict(list)

	for line in sys.stdin:
		key, value_type, value = line.strip().split('\t')

		if current_key is not None and key != current_key:
			process_key_data(current_key, key_data[current_key])
			key_data.pop(current_key, None)

		current_key = key
		key_data[current_key].append((value_type, value))

	if current_key is not None:
		process_key_data(current_key, key_data[current_key])

	flush_batches(True)
	print("Reducer finished processing.")