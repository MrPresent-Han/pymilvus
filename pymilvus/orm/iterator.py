
class QueryIterator:

    def __init__(self, connection, collection_name, expr, output_fields=None, partition_names=None, schema=None, timeout=None, **kwargs):
        self._conn = connection
        self._collection_name = collection_name
        self._expr = expr
        self._output_fields = output_fields
        self._partition_names = partition_names
        self._schema = schema
        self._timeout = timeout
        self._kwargs = kwargs
        self.__seek()
        pass

    def __seek(self):
        self.conn.query(self._collection_name, self._expr, self._output_fields, self._partition_names,
                         timeout=self.timeout, schema=self.schema, **self.kwargs)
        pass

    def next(self):
        pass

    def close(self):
        pass


class SearchIterator:

    def __init__(self, conn):
        pass

    def __seek(self):
        pass

    def next(self):
        pass

    def close(self):
        pass
