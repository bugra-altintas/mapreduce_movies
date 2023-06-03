import csv
import sys

def convert_csv_to_tsv(csv_file, tsv_file):
    with open(csv_file, 'r') as file_in:
        csv_reader = csv.reader(file_in)
        with open(tsv_file, 'w', newline='') as file_out:
            tsv_writer = csv.writer(file_out, delimiter='\t')
            for row in csv_reader:
                tsv_writer.writerow(row)

# Example usage
csv_file = sys.argv[1]
tsv_file = sys.argv[1].replace('.csv', '.tsv')
convert_csv_to_tsv(csv_file, tsv_file)
