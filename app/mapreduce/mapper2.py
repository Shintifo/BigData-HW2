import sys

for line in sys.stdin:
	line = line.strip()
	if not line:
		continue

	print(line)
	parts = line.split('\t')

	if parts[0].startswith('DOC_'):
		continue

	term, id, tf = parts
	print(f"{term}\tDF\t1")