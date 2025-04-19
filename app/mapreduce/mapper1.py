import os
from collections import Counter
import re
import sys

def count_tf():
	for content in sys.stdin:
		input_file = os.getenv('mapreduce_map_input_file', 'unknown_file')
		filepath = input_file.strip()
		name = filepath.split("/")[-1]
		id = int(name.split("_")[0])

		tokens = re.findall(r'\w+', content.lower())
		doc_len = len(tokens)

		print(f"DOC_{id}\tLEN\t{doc_len}")

		term_counts = Counter(tokens)

		for term, count in term_counts.items():
			print(f"{term}\t{id}\t{count}")

if __name__ == "__main__":
	# try:
	count_tf()
	# except Exception as e:
	# 	print(e)