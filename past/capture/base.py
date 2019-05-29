class Capture(object):

    def __init__(self, mongo_client, db_cursor):
        self.mongo_client = mongo_client
        self.db_cursor = db_cursor

    def query_sql(self, sql):
        # query records and columns.
        # try:
        self.db_cursor.execute(sql)
        records = self.db_cursor.fetchall()
        columns = [value[0] for value in self.db_cursor.description]
        return records, columns
        # except Exception as err:
        #     print(sql)
        #     raise err
