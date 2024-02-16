import argparse
import duckdb
import pyoxigraph
import io
import sys

def main():
    parser = argparse.ArgumentParser(description="Process some arguments.")
    parser.add_argument("--sourcedir", type=str, help="Source directory for parquet files")
    parser.add_argument("--sourcefile", type=str, help="Source parquet file")
    parser.add_argument("--outputfile", type=str, help="Output graph file")

    args = parser.parse_args()
    if args.sourcedir is None and  args.sourcefile is None:
        print("Error: one of --sourcedir or --sourcefile  is required")
        sys.exit(1)
    if args.outputfile is None:
        print("Error:  --outputfile is required")
        sys.exit(1)

    sdir = args.sourcedir
    sfile = args.sourcefile
    ofile = args.outputfile

    # Instantiate the DuckDB connection
    con = duckdb.connect()

    mime_type = "application/n-triples"
    store = pyoxigraph.Store(path="./store")

    store.clear()  # TODO:  make this a boolean flag in case you want to keep the store dir around

    if sdir is not None:
        print("Indexing directory: {}".format(sdir))
        con.execute("SELECT rdf FROM read_parquet('{}/*.parquet')".format(sdir))  # Replace with your query
    else:
        print("Indexing file: {}".format(sfile))
        con.execute("SELECT rdf FROM read_parquet('{}')".format(sfile))  # Replace with your query

    while True:
        row = con.fetchone()
        if row is None:
            break
        store.load(io.StringIO(row[0]), mime_type, base_iri=None, to_graph=None)

    output_mime_type = "application/n-quads"
    store.dump("{}".format(ofile), output_mime_type)


if __name__ == '__main__':
    main()