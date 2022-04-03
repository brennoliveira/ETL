import cx_Oracle

#NOT WORKING
class OracleConnection(object):
    def __init__(self):
        self.SERVER = 'oracle-74473-0.cloudclusters.net'
        self.PORT = '12272' # your port
        self.USER = 'ADMIN'
        self.PASSWORD = '123456789'
        self.DATABASE = 'XE'

    def connect_oracle(self):
        conn = cx_Oracle.connect(self.USER, self.PASSWORD,
        '{}:{}/{}'.format(self.SERVER, self.PORT, self.DATABASE),
        cx_Oracle.SYSDBA)
        return conn

    def operate_database(self):
        # example select login user
        connect = self.connect_oracle()
        curs = connect.cursor()
        sql = "select * from SOCIOS"
        curs.execute(sql)
        row = curs.fetchone()
        print(row[0])
        curs.close()
        connect.close()


if __name__ == '__main__':
    OracleConnection().operate_database()