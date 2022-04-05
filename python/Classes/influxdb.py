from influxdb_client import InfluxDBClient

class influxDB:
    def __init__(self, token, org, port):
        self.token = token
        self.org  = org
        self.port = port
        

    def _clientConnect(self):
        client = InfluxDBClient(
            url=f'http://localhost{self.port}',
            token = self.token,
            org = self.org
            )
        print('Connection Established')
        return client

    def _writeAPI(self, client):
        write_api = client.write_api()
        return write_api

    def _readAPI(self, client):
        read_api = client.query_api()
        return read_api

    def execute_batchQuery(self, query, accessor):
        client = self._clientConnect()
        queryResult = self._readAPI(client).query(org = self.org, query = query)
        if accessor is 'dataframe':
            return self._accessor_method(accessor, queryResult)
        elif type(accessor) == list:
            results = {}
            table_loop = list(map(lambda x: x.records, queryResult))
            for record in table_loop:
                i = 0
                while i < len(accessor):
                    if accessor not in results.keys():
                        results[accessor[i]] = []
                    else:
                        results[accessor[i]].append(self._acessor_method(accessor[i], record))
                    i += 1
            return results

    def _acessor_method(self, accessor, queryResult):
        '''
        dataframe argument returns a pandas DataFrame object, while other 
        accessor methods are appended to an array
        '''
        queryDict = {
            'measurement' : queryResult.get_measurement(),
            'field' : queryResult.get_field(),
            'time' : queryResult.get_time(),
            'start' : queryResult.get_start(),
            'stop' : queryResult.get_stop(),
            'dataframe' : queryResult.query_data_frame()
        }
        return queryDict[accessor]