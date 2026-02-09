.PHONY: test

test:
	gotestsum --format testname -- ./wal/...