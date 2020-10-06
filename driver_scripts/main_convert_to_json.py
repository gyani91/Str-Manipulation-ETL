import os
import re
import json

if __name__ == "__main__":
    scripts_root = "../sql_scripts/"

    # Initialize an empty dictionary for storing the tables and their column names
    schema = dict()

    # Initialize regex
    delimiters = "select ", "\nfrom ", "\nwhere "
    regex_pattern = '|'.join(map(re.escape, delimiters))

    delimiters_cond_ops = " and ", " or "
    regex_pattern_cond_ops = '|'.join(map(re.escape, delimiters_cond_ops))

    # Search for sql files in subfolders
    for root, _, files in sorted(os.walk(scripts_root)):
        # loop over files in a directory
        for filename in sorted(files):
            filepath = os.path.join(root, filename)
            if filepath.endswith('.sql'):
                with open(filepath, "r") as f:
                    sql = f.read()
                    clauses = re.split(regex_pattern, sql)

                    # column names from select clause
                    columns = set()

                    parts = clauses[1].split(',')
                    for part in parts:
                        if part.replace(' ', '').startswith('{{'):
                            columns.add(re.findall(r'\'(.+?)\'', part)[0])
                        else:
                            column = part.split()[0]
                            # check if the column is not a function, like timestamp()
                            if '(' not in column:
                                columns.add(column)

                    # table name from the from clause
                    tablename = re.findall(r'\'(.+?)\'', clauses[2])[-1]

                    # column names from where clause
                    parts = re.split(regex_pattern_cond_ops, clauses[3])

                    for part in parts:
                        columns.add(part.split()[1] if part.split()[0].lower() == 'not' else part.split()[0])

                    # add the table and its column names to the dictionary
                    if tablename in schema:
                        schema[tablename] = list(columns.union(set(schema[tablename])))
                    else:
                        schema[tablename] = list(columns)

    # Dump the dictionary to a json file to be read later programmatically by an ETL pipeline
    with open('../result/legacy.json', 'w') as fp:
        json.dump(schema, fp, indent=4)


