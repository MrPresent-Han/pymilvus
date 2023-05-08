from .constants import OFFSET, LIMIT, ID


class QueryIterator:

    def __init__(self, connection, collection_name, expr, output_fields=None, partition_names=None, schema=None,
                 timeout=None, **kwargs):
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
        if self._kwargs.get(OFFSET, 0) == 0:
            # hc-- what if id can be less than 0?
            self._next_id = -1
            return

        first_cursor_kwargs = self._kwargs.copy()
        first_cursor_kwargs[OFFSET] = 0
        # hc--may be offset too large
        first_cursor_kwargs[LIMIT] = self._kwargs[OFFSET]

        res = self._conn.query(self._collection_name, self._expr, self._output_fields, self._partition_names,
                              timeout=self._timeout, **first_cursor_kwargs)
        self.__set_cursor(res)
        self._kwargs[OFFSET] = 0

    def next(self):
        current_expr = self._expr + f" and id > {self._next_id}"
        print(f"current_expr:{current_expr}")
        res = self._conn.query(self._collection_name, current_expr, self._output_fields, self._partition_names,
                              timeout=self._timeout, **self._kwargs)
        self.__set_cursor(res)
        return res

    def __set_cursor(self, res):
        if len(res) == 0:
            return
        # hc--what about pk is varchar?
        self._next_id = res[-1][ID]

    def close(self):
        # do nothing for the moment, if iterator freeze snapshot on milvus server side
        # in the future, close function need to release the distribution snapshot accordingly
        return


class SearchIterator:

    def __init__(self, conn):
        pass

    def __seek(self):
        pass

    def next(self):
        pass

    def close(self):
        pass
