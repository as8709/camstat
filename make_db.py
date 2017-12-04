import argparse

import camstat

if __name__=="__main__":
    parser = argparse.ArgumentParser(description="Script for loading data from anpr spreadsheets into a db")
    parser.add_argument("xlsx_dir", help="path to the directory where the spreadsheets are")
    parser.add_argument("dbname", help="name of the db to create")
    parser.add_argument("password", help="password to the database")
    parser.add_argument("-c", "--create_table", action="store_true", help="flag for whether to create the journeys table")
    args = parser.parse_args()
    if args.create_table:
        camstat.make_journeys_table(args.dbname, args.password)
    print("Loading data into database, this may take some time")
    for spreadsheet in glob.glob(os.path.join(os.path.abspath(args.xlsx_dir), "*.xlsx")):
        wb = openpyxl.load_workbook(filename=spreadsheet, read_only=True)
        camstat.DataLoader(wb, args.dbname, args.password).load()
        print("loaded:{}".format(spreadsheet))
