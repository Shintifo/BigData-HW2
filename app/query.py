import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum, greatest, log, max as spark_max, count, avg

from app import CASSANDRA_KEYSPACE

k1 = 1.5
b = 0.75


def tokenize(text):
	text = text.lower()
	text = re.sub(r'[^\w\s]', ' ', text)
	return text.split()


def compute_bm25(bm25_data, corpus_stats):
	total_docs = corpus_stats["total_docs"]
	avg_doc_length = corpus_stats["avg_doc_length"]

	epsilon = 1e-6

	bm25_scores = bm25_data.withColumn(
		"idf",
		greatest(lit(0.0), log((lit(total_docs) - col("doc_count") + 0.5) / (col("doc_count") + 0.5 + epsilon)))
	).withColumn(
		"length_normalization",
		(1 - b + b * col("doc_length") / (lit(avg_doc_length) + epsilon))  # Add epsilon here too
	).withColumn(
		"tf_normalization",
		(col("tf") * (k1 + 1)) / (col("tf") + k1 * col("length_normalization") + epsilon)  # Add epsilon here too
	).withColumn(
		"bm25_score_term",
		col("idf") * col("tf_normalization")
	)

	doc_scores_df = bm25_scores.groupBy("doc_id").agg(
		spark_sum("bm25_score_term").alias("total_bm25_score")
	)

	ranked_docs_df = doc_scores_df.orderBy(col("total_bm25_score").desc())

	return ranked_docs_df


def main():
	query = ' '.join(sys.argv[1:])
	print(f"Query: {query}")

	query_terms = tokenize(query)

	spark = SparkSession.builder \
		.appName("BM25 Search") \
		.config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1") \
		.config("spark.cassandra.connection.host", "cassandra-server") \
		.getOrCreate()

	try:
		print("One")
		doc_stats_df = spark.read.format("org.apache.spark.sql.cassandra") \
			.options(table="doc_stats", keyspace=CASSANDRA_KEYSPACE) \
			.load()

		print("Two")
		corpus_stats = doc_stats_df.agg(
			count("doc_id").alias("total_docs"),
			avg("doc_length").alias("avg_doc_length")
		).first()

		print("Three")
		term_stats_df = spark.read.format("org.apache.spark.sql.cassandra") \
			.options(table="vocab_stats", keyspace=CASSANDRA_KEYSPACE) \
			.load() \
			.filter(col("term").isin(query_terms)) \
			.select("term", col("doc_frequency").alias("doc_count"))

		print("Four")
		doc_index_df = spark.read.format("org.apache.spark.sql.cassandra") \
			.options(table="inv_index", keyspace=CASSANDRA_KEYSPACE) \
			.load() \
			.filter(col("term").isin(query_terms)) \
			.select("term", "doc_id",
					col("term_frequency").alias("tf"))

		print("Five")
		indexed_terms_stats_df = doc_index_df.join(term_stats_df, "term", "left_outer") \
			.select(
			doc_index_df["*"],
			col("doc_count")
		)

		print("Six")
		bm25_data = indexed_terms_stats_df.join(doc_stats_df.select("doc_id", "doc_length"), "doc_id", "inner") \
			.select(
			indexed_terms_stats_df["*"],
			col("doc_length")
		)

		print("Seven")
		ranked_docs_df = compute_bm25(bm25_data, corpus_stats)

		results = ranked_docs_df.limit(10).collect()
		print("\nTop 10 relevant documents:")
		print("--------------------------")

		if results:
			for i, row in enumerate(results):
				print(f"{i + 1}. Document ID: {row['doc_id']}, Score: {row['total_bm25_score']:.4f}")
		else:
			print("No relevant documents found for the query.\n")

	except Exception as e:
		print(f"An error occurred: {e}")
		import traceback
		traceback.print_exc()

	finally:
		spark.stop()


if __name__ == "__main__":
	main()
