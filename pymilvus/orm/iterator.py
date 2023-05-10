from .constants import OFFSET, LIMIT, ID, FIELDS
from .types import DataType


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
        self.__setup__pk_is_str()
        self.__seek()

    def __seek(self):
        if self._kwargs.get(OFFSET, 0) == 0:
            self._next_id = None
            return

        first_cursor_kwargs = self._kwargs.copy()
        first_cursor_kwargs[OFFSET] = 0
        # offset may be too large
        first_cursor_kwargs[LIMIT] = self._kwargs[OFFSET]

        res = self._conn.query(self._collection_name, self._expr, self._output_fields, self._partition_names,
                               timeout=self._timeout, **first_cursor_kwargs)
        self.__set_cursor(res)
        self._kwargs[OFFSET] = 0

    def next(self):
        current_expr = self.__setup_next_expr()
        res = self._conn.query(self._collection_name, current_expr, self._output_fields, self._partition_names,
                               timeout=self._timeout, **self._kwargs)
        self.__update_cursor(res)
        return res

    def __setup__pk_is_str(self):
        fields = self._schema[FIELDS]
        for field in fields:
            if field['is_primary']:
                if field['type'] == DataType.VARCHAR:
                    self._pk_str = True
                else:
                    self._pk_str = False
                break

    def __setup_next_expr(self):
        current_expr = self._expr
        if self._next_id is None:
            return current_expr
        if self._next_id is not None:
            if self._pk_str:
                current_expr = self._expr + f" and id > \"{self._next_id}\""
            else:
                current_expr = self._expr + f" and id > {self._next_id}"
        return current_expr

    def __update_cursor(self, res):
        if len(res) == 0:
            return
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
