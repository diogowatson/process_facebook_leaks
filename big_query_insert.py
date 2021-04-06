from google.cloud import bigquery
import argparse


class DataInsert:

    LIMIT = 10000
    RETRY_LIMIT = 5

    def __init__(self,table_name, service_account_json,file_name,project='facebookleaks',dataset='facebookLeaks2021'):
        self.table_name = table_name
        self.project = project
        self.dataset = dataset
        self.file = file_name
        self.table_id = f'{self.project}.{self.dataset}.{table_name}'
        # create big query client
        self.client = bigquery.Client.from_service_account_json(service_account_json)
        self.rows_to_insert = []
        self.count = 0

    def insert_values(self, numbers, name, gender):
        query = (f"insert `{self.project}.{self.dataset}.{table_name}` (numbers,name,gender) values ('{numbers}','{name}','{gender}')")
        query_job = self.client.query(query)
        print(query_job.result)

    def clean_count(self):
        self.count = 0

    def clean_rows_to_insert(self):
        self.rows_to_insert = []

    def append_values(self,number_append: str, name_append: str, gender_append: str)->dict:
        return {u'numbers':number_append, u'name':name_append, u'gender':gender_append}

    def process_line(self, line):
        number = ''
        name = ''
        gender = ''
        try:
            splited = line.split(':')
            number = splited[1]
            name = splited[2] + ' ' + splited[3]
            gender = splited[4]
        except Exception as e:
            print(e)
        return number, name, gender

    def create_table(self):
        self.client.query(f"create table if not exists `{self.table_id}` (numbers STRING, name STRING, gender STRING)")

    def save_storage(self):
        error_count = 1
        errors = self.client.insert_rows_json(self.table_id, self.rows_to_insert)  # Make an API request.
        while error_count >= DataInsert.RETRY_LIMIT and errors != []:
            print("Encountered errors while inserting rows: {}".format(errors))
            print("Trying to retry the data")
            print("tentative : ", error_count, "max number ", DataInsert.RETRY_LIMIT)
            errors = self.client.insert_rows_json(self.table_id, self.rows_to_insert)
            error_count += 1

        if not errors:
            print("New rows have been added.")
        self.clean_count()
        self.clean_rows_to_insert()

    def run(self):
        self.create_table()
        with open(self.file) as f:
            for file_line in f.readlines():
                if self.count < DataInsert.LIMIT:
                    numbers, name, gender = self.process_line(file_line)
                    self.rows_to_insert.append(self.append_values(numbers,name, gender))
                    self.count +=1
                else:
                    self.save_storage()
            self.save_storage()
        print("Process Completed")


def create_argument_parser():
    """
    argument parser
    basic usage:
        parser = create_argument_parser()
        args = parser.parse_args()
    """
    arg_parser = argparse.ArgumentParser(description='Script to parser arguments')
    arg_parser.add_argument('service_account',
                            type=str,
                            help='google service account json path')
    arg_parser.add_argument('file',
                            type=str,
                            help='input file path')
    arg_parser.add_argument('country',
                            type=str,
                            help='name of the country or region to be extracted')

    return arg_parser


if __name__ == '__main__':
    # get arguments
    parser = create_argument_parser()
    args = parser.parse_args()

    #get table name
    table_name = args.country

    # create BigQuery client
    data_insert = DataInsert(table_name, args.service_account, args.file)
    data_insert.run()