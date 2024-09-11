import base64
import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Union

import numpy as np

from pymilvus.exceptions import DataNotMatchException, ExceptionsMessage, ParamError
from pymilvus.grpc_gen import common_pb2 as common_types
from pymilvus.grpc_gen import milvus_pb2 as milvus_types
from pymilvus.grpc_gen import schema_pb2 as schema_types
from pymilvus.orm.schema import CollectionSchema

from . import __version__, blob, entity_helper, ts_utils, utils
from .check import check_pass_param, is_legal_collection_properties
from .constants import (
    DEFAULT_CONSISTENCY_LEVEL,
    GROUP_BY_FIELD,
    GROUP_SIZE,
    GROUP_STRICT_SIZE,
    ITERATOR_FIELD,
    PAGE_RETAIN_ORDER_FIELD,
    RANK_GROUP_SCORER,
    REDUCE_STOP_FOR_BEST,
)
from .types import (
    DataType,
    PlaceholderType,
    ResourceGroupConfig,
    get_consistency_level,
)
from .utils import traverse_info, traverse_rows_info, traverse_upsert_info


class Prepare:
    @classmethod
    def create_collection_request(
        cls,
        collection_name: str,
        fields: Union[Dict[str, Iterable], CollectionSchema],
        **kwargs,
    ) -> milvus_types.CreateCollectionRequest:
        """
        Args:
            fields (Union(Dict[str, Iterable], CollectionSchema)).

                {"fields": [
                        {"name": "A", "type": DataType.INT32}
                        {"name": "B", "type": DataType.INT64, "auto_id": True, "is_primary": True},
                        {"name": "C", "type": DataType.FLOAT},
                        {"name": "Vec", "type": DataType.FLOAT_VECTOR, "params": {"dim": 128}}]
                }

        Returns:
            milvus_types.CreateCollectionRequest
        """

        if isinstance(fields, CollectionSchema):
            schema = cls.get_schema_from_collection_schema(collection_name, fields)
        else:
            schema = cls.get_schema(collection_name, fields, **kwargs)

        consistency_level = get_consistency_level(
            kwargs.get("consistency_level", DEFAULT_CONSISTENCY_LEVEL)
        )

        req = milvus_types.CreateCollectionRequest(
            collection_name=collection_name,
            schema=bytes(schema.SerializeToString()),
            consistency_level=consistency_level,
        )

        properties = kwargs.get("properties")
        if is_legal_collection_properties(properties):
            properties = [
                common_types.KeyValuePair(key=str(k), value=str(v)) for k, v in properties.items()
            ]
            req.properties.extend(properties)

        same_key = set(kwargs.keys()).intersection({"num_shards", "shards_num"})
        if len(same_key) > 0:
            if len(same_key) > 1:
                msg = "got both num_shards and shards_num in kwargs, expected only one of them"
                raise ParamError(message=msg)

            num_shards = kwargs[next(iter(same_key))]
            if not isinstance(num_shards, int):
                msg = f"invalid num_shards type, got {type(num_shards)}, expected int"
                raise ParamError(message=msg)
            req.shards_num = num_shards

        num_partitions = kwargs.get("num_partitions", None)
        if num_partitions is not None:
            if not isinstance(num_partitions, int) or isinstance(num_partitions, bool):
                msg = f"invalid num_partitions type, got {type(num_partitions)}, expected int"
                raise ParamError(message=msg)
            if num_partitions < 1:
                msg = f"The specified num_partitions should be greater than or equal to 1, got {num_partitions}"
                raise ParamError(message=msg)
            req.num_partitions = num_partitions

        return req

    @classmethod
    def get_schema_from_collection_schema(
        cls,
        collection_name: str,
        fields: CollectionSchema,
    ) -> schema_types.CollectionSchema:
        coll_description = fields.description
        if not isinstance(coll_description, (str, bytes)):
            msg = (
                f"description [{coll_description}] has type {type(coll_description).__name__}, "
                "but expected one of: bytes, str"
            )
            raise ParamError(message=msg)

        schema = schema_types.CollectionSchema(
            name=collection_name,
            autoID=fields.auto_id,
            description=coll_description,
            enable_dynamic_field=fields.enable_dynamic_field,
        )
        for f in fields.fields:
            field_schema = schema_types.FieldSchema(
                name=f.name,
                data_type=f.dtype,
                description=f.description,
                is_primary_key=f.is_primary,
                default_value=f.default_value,
                nullable=f.nullable,
                autoID=f.auto_id,
                is_partition_key=f.is_partition_key,
                is_dynamic=f.is_dynamic,
                element_type=f.element_type,
                is_clustering_key=f.is_clustering_key,
            )
            for k, v in f.params.items():
                kv_pair = common_types.KeyValuePair(
                    key=str(k) if k != "mmap_enabled" else "mmap.enabled", value=str(v)
                )
                field_schema.type_params.append(kv_pair)

            schema.fields.append(field_schema)
        return schema

    @staticmethod
    def get_field_schema(
        field: Dict,
        primary_field: Any,
        auto_id_field: Any,
    ) -> (schema_types.FieldSchema, Any, Any):
        field_name = field.get("name")
        if field_name is None:
            raise ParamError(message="You should specify the name of field!")

        data_type = field.get("type")
        if data_type is None:
            raise ParamError(message="You should specify the data type of field!")
        if not isinstance(data_type, (int, DataType)):
            raise ParamError(message="Field type must be of DataType!")

        is_primary = field.get("is_primary", False)
        if not isinstance(is_primary, bool):
            raise ParamError(message="is_primary must be boolean")
        if is_primary:
            if primary_field is not None:
                raise ParamError(message="A collection should only have one primary field")
            if DataType(data_type) not in [DataType.INT64, DataType.VARCHAR]:
                msg = "int64 and varChar are the only supported types of primary key"
                raise ParamError(message=msg)
            primary_field = field_name

        nullable = field.get("nullable", False)
        if not isinstance(nullable, bool):
            raise ParamError(message="nullable must be boolean")

        auto_id = field.get("auto_id", False)
        if not isinstance(auto_id, bool):
            raise ParamError(message="auto_id must be boolean")
        if auto_id:
            if auto_id_field is not None:
                raise ParamError(message="A collection should only have one autoID field")
            if DataType(data_type) != DataType.INT64:
                msg = "int64 is the only supported type of automatic generated id"
                raise ParamError(message=msg)
            auto_id_field = field_name

        field_schema = schema_types.FieldSchema(
            name=field_name,
            data_type=data_type,
            description=field.get("description", ""),
            is_primary_key=is_primary,
            autoID=auto_id,
            is_partition_key=field.get("is_partition_key", False),
            is_clustering_key=field.get("is_clustering_key", False),
        )

        type_params = field.get("params", {})
        if not isinstance(type_params, dict):
            raise ParamError(message="params should be dictionary type")
        kvs = [
            common_types.KeyValuePair(
                key=str(k) if k != "mmap_enabled" else "mmap.enabled", value=str(v)
            )
            for k, v in type_params.items()
        ]
        field_schema.type_params.extend(kvs)

        return field_schema, primary_field, auto_id_field

    @classmethod
    def get_schema(
        cls,
        collection_name: str,
        fields: Dict[str, Iterable],
        **kwargs,
    ) -> schema_types.CollectionSchema:
        if not isinstance(fields, dict):
            raise ParamError(message="Param fields must be a dict")

        all_fields = fields.get("fields")
        if all_fields is None:
            raise ParamError(message="Param fields must contain key 'fields'")
        if len(all_fields) == 0:
            raise ParamError(message="Param fields value cannot be empty")

        enable_dynamic_field = kwargs.get("enable_dynamic_field", False)
        if "enable_dynamic_field" in fields:
            enable_dynamic_field = fields["enable_dynamic_field"]

        schema = schema_types.CollectionSchema(
            name=collection_name,
            autoID=False,
            description=fields.get("description", ""),
            enable_dynamic_field=enable_dynamic_field,
        )

        primary_field, auto_id_field = None, None
        for field in all_fields:
            (field_schema, primary_field, auto_id_field) = cls.get_field_schema(
                field, primary_field, auto_id_field
            )

            schema.fields.append(field_schema)
        return schema

    @classmethod
    def drop_collection_request(cls, collection_name: str) -> milvus_types.DropCollectionRequest:
        return milvus_types.DropCollectionRequest(collection_name=collection_name)

    @classmethod
    def describe_collection_request(
        cls,
        collection_name: str,
    ) -> milvus_types.DescribeCollectionRequest:
        return milvus_types.DescribeCollectionRequest(collection_name=collection_name)

    @classmethod
    def alter_collection_request(
        cls,
        collection_name: str,
        properties: Dict,
    ) -> milvus_types.AlterCollectionRequest:
        kvs = [common_types.KeyValuePair(key=k, value=str(v)) for k, v in properties.items()]

        return milvus_types.AlterCollectionRequest(collection_name=collection_name, properties=kvs)

    @classmethod
    def collection_stats_request(cls, collection_name: str):
        return milvus_types.CollectionStatsRequest(collection_name=collection_name)

    @classmethod
    def show_collections_request(cls, collection_names: Optional[List[str]] = None):
        req = milvus_types.ShowCollectionsRequest()
        if collection_names:
            if not isinstance(collection_names, (list,)):
                msg = f"collection_names must be a list of strings, but got: {collection_names}"
                raise ParamError(message=msg)
            for collection_name in collection_names:
                check_pass_param(collection_name=collection_name)
            req.collection_names.extend(collection_names)
            req.type = milvus_types.ShowType.InMemory
        return req

    @classmethod
    def rename_collections_request(cls, old_name: str, new_name: str, new_db_name: str):
        return milvus_types.RenameCollectionRequest(
            oldName=old_name, newName=new_name, newDBName=new_db_name
        )

    @classmethod
    def create_partition_request(cls, collection_name: str, partition_name: str):
        return milvus_types.CreatePartitionRequest(
            collection_name=collection_name, partition_name=partition_name
        )

    @classmethod
    def drop_partition_request(cls, collection_name: str, partition_name: str):
        return milvus_types.DropPartitionRequest(
            collection_name=collection_name, partition_name=partition_name
        )

    @classmethod
    def has_partition_request(cls, collection_name: str, partition_name: str):
        return milvus_types.HasPartitionRequest(
            collection_name=collection_name, partition_name=partition_name
        )

    @classmethod
    def partition_stats_request(cls, collection_name: str, partition_name: str):
        return milvus_types.PartitionStatsRequest(
            collection_name=collection_name, partition_name=partition_name
        )

    @classmethod
    def show_partitions_request(
        cls,
        collection_name: str,
        partition_names: Optional[List[str]] = None,
        type_in_memory: bool = False,
    ):
        check_pass_param(collection_name=collection_name, partition_name_array=partition_names)
        req = milvus_types.ShowPartitionsRequest(collection_name=collection_name)
        if partition_names:
            if not isinstance(partition_names, (list,)):
                msg = f"partition_names must be a list of strings, but got: {partition_names}"
                raise ParamError(msg)
            for partition_name in partition_names:
                check_pass_param(partition_name=partition_name)
            req.partition_names.extend(partition_names)
        if type_in_memory is False:
            req.type = milvus_types.ShowType.All
        else:
            req.type = milvus_types.ShowType.InMemory
        return req

    @classmethod
    def get_loading_progress(
        cls, collection_name: str, partition_names: Optional[List[str]] = None
    ):
        check_pass_param(collection_name=collection_name, partition_name_array=partition_names)
        req = milvus_types.GetLoadingProgressRequest(collection_name=collection_name)
        if partition_names:
            req.partition_names.extend(partition_names)
        return req

    @classmethod
    def get_load_state(cls, collection_name: str, partition_names: Optional[List[str]] = None):
        check_pass_param(collection_name=collection_name, partition_name_array=partition_names)
        req = milvus_types.GetLoadStateRequest(collection_name=collection_name)
        if partition_names:
            req.partition_names.extend(partition_names)
        return req

    @classmethod
    def empty(cls):
        msg = "no empty request later"
        raise DeprecationWarning(msg)

    @classmethod
    def register_link_request(cls):
        return milvus_types.RegisterLinkRequest()

    @classmethod
    def partition_name(cls, collection_name: str, partition_name: str):
        if not isinstance(collection_name, str):
            raise ParamError(message="collection_name must be of str type")
        if not isinstance(partition_name, str):
            raise ParamError(message="partition_name must be of str type")
        return milvus_types.PartitionName(collection_name=collection_name, tag=partition_name)

    @staticmethod
    def _parse_row_request(
        request: Union[milvus_types.InsertRequest, milvus_types.UpsertRequest],
        fields_info: dict,
        enable_dynamic: bool,
        entities: List,
    ):
        fields_data = {
            field["name"]: schema_types.FieldData(field_name=field["name"], type=field["type"])
            for field in fields_info
            if not field.get("auto_id", False)
        }
        field_info_map = {
            field["name"]: field for field in fields_info if not field.get("auto_id", False)
        }

        if enable_dynamic:
            d_field = schema_types.FieldData(is_dynamic=True, type=DataType.JSON)
            fields_data[d_field.field_name] = d_field
            field_info_map[d_field.field_name] = d_field

        try:
            for entity in entities:
                if not isinstance(entity, Dict):
                    msg = f"expected Dict, got '{type(entity).__name__}'"
                    raise TypeError(msg)
                for k, v in entity.items():
                    if k not in fields_data and not enable_dynamic:
                        raise DataNotMatchException(
                            message=ExceptionsMessage.InsertUnexpectedField % k
                        )

                    if k in fields_data:
                        field_info, field_data = field_info_map[k], fields_data[k]
                        if field_info.get("nullable", False) or field_info.get(
                            "default_value", None
                        ):
                            field_data.valid_data.append(v is not None)
                        if v is not None:
                            entity_helper.pack_field_value_to_field_data(v, field_data, field_info)
                json_dict = {
                    k: v for k, v in entity.items() if k not in fields_data and enable_dynamic
                }

                if enable_dynamic:
                    json_value = entity_helper.convert_to_json(json_dict)
                    d_field.scalars.json_data.data.append(json_value)

        except (TypeError, ValueError) as e:
            raise DataNotMatchException(message=ExceptionsMessage.DataTypeInconsistent) from e

        request.fields_data.extend(
            [fields_data[field["name"]] for field in fields_info if not field.get("auto_id", False)]
        )

        if enable_dynamic:
            request.fields_data.append(d_field)

        _, _, auto_id_loc = traverse_rows_info(fields_info, entities)
        if auto_id_loc is not None:
            if (enable_dynamic and len(fields_data) != len(fields_info)) or (
                not enable_dynamic and len(fields_data) + 1 != len(fields_info)
            ):
                raise ParamError(ExceptionsMessage.FieldsNumInconsistent)
        elif enable_dynamic and len(fields_data) != len(fields_info) + 1:
            raise ParamError(ExceptionsMessage.FieldsNumInconsistent)
        return request

    @classmethod
    def row_insert_param(
        cls,
        collection_name: str,
        entities: List,
        partition_name: str,
        fields_info: Dict,
        enable_dynamic: bool = False,
    ):
        if not fields_info:
            raise ParamError(message="Missing collection meta to validate entities")

        # insert_request.hash_keys won't be filled in client.
        p_name = partition_name if isinstance(partition_name, str) else ""
        request = milvus_types.InsertRequest(
            collection_name=collection_name,
            partition_name=p_name,
            num_rows=len(entities),
        )

        return cls._parse_row_request(request, fields_info, enable_dynamic, entities)

    @classmethod
    def row_upsert_param(
        cls,
        collection_name: str,
        entities: List,
        partition_name: str,
        fields_info: Any,
        enable_dynamic: bool = False,
    ):
        if not fields_info:
            raise ParamError(message="Missing collection meta to validate entities")

        # upsert_request.hash_keys won't be filled in client.
        p_name = partition_name if isinstance(partition_name, str) else ""
        request = milvus_types.UpsertRequest(
            collection_name=collection_name,
            partition_name=p_name,
            num_rows=len(entities),
        )

        return cls._parse_row_request(request, fields_info, enable_dynamic, entities)

    @staticmethod
    def _pre_insert_batch_check(
        entities: List,
        fields_info: Any,
    ):
        for entity in entities:
            if (
                entity.get("name") is None
                or entity.get("values") is None
                or entity.get("type") is None
            ):
                raise ParamError(
                    message="Missing param in entities, a field must have type, name and values"
                )
        if not fields_info:
            raise ParamError(message="Missing collection meta to validate entities")

        location, primary_key_loc, auto_id_loc = traverse_info(fields_info)

        # though impossible from sdk
        if primary_key_loc is None:
            raise ParamError(message="primary key not found")

        if auto_id_loc is None and len(entities) != len(fields_info):
            msg = f"number of fields: {len(fields_info)}, number of entities: {len(entities)}"
            raise ParamError(msg)

        if auto_id_loc is not None and len(entities) + 1 != len(fields_info):
            msg = f"number of fields: {len(fields_info)}, number of entities: {len(entities)}"
            raise ParamError(msg)
        return location

    @staticmethod
    def _pre_upsert_batch_check(
        entities: List,
        fields_info: Any,
    ):
        for entity in entities:
            if (
                entity.get("name") is None
                or entity.get("values") is None
                or entity.get("type") is None
            ):
                raise ParamError(
                    message="Missing param in entities, a field must have type, name and values"
                )
        if not fields_info:
            raise ParamError(message="Missing collection meta to validate entities")

        location, primary_key_loc = traverse_upsert_info(fields_info)

        # though impossible from sdk
        if primary_key_loc is None:
            raise ParamError(message="primary key not found")

        if len(entities) != len(fields_info):
            msg = f"number of fields: {len(fields_info)}, number of entities: {len(entities)}"
            raise ParamError(msg)
        return location

    @staticmethod
    def _parse_batch_request(
        request: Union[milvus_types.InsertRequest, milvus_types.UpsertRequest],
        entities: List,
        fields_info: Any,
        location: Dict,
    ):
        pre_field_size = 0
        try:
            for entity in entities:
                latest_field_size = entity_helper.get_input_num_rows(entity.get("values"))
                if latest_field_size != 0:
                    if pre_field_size not in (0, latest_field_size):
                        raise ParamError(
                            message=(
                                f"Field data size misaligned for field [{entity.get('name')}] ",
                                f"got size=[{latest_field_size}] ",
                                f"alignment size=[{pre_field_size}]",
                            )
                        )
                    pre_field_size = latest_field_size
            if pre_field_size == 0:
                raise ParamError(message=ExceptionsMessage.NumberRowsInvalid)
            request.num_rows = pre_field_size
            for entity in entities:
                field_data = entity_helper.entity_to_field_data(
                    entity, fields_info[location[entity.get("name")]], request.num_rows
                )
                request.fields_data.append(field_data)
        except (TypeError, ValueError) as e:
            raise DataNotMatchException(message=ExceptionsMessage.DataTypeInconsistent) from e

        if pre_field_size == 0:
            raise ParamError(message=ExceptionsMessage.NumberRowsInvalid)
        request.num_rows = pre_field_size
        return request

    @classmethod
    def batch_insert_param(
        cls,
        collection_name: str,
        entities: List,
        partition_name: str,
        fields_info: Any,
    ):
        location = cls._pre_insert_batch_check(entities, fields_info)
        tag = partition_name if isinstance(partition_name, str) else ""
        request = milvus_types.InsertRequest(collection_name=collection_name, partition_name=tag)

        return cls._parse_batch_request(request, entities, fields_info, location)

    @classmethod
    def batch_upsert_param(
        cls,
        collection_name: str,
        entities: List,
        partition_name: str,
        fields_info: Any,
    ):
        location = cls._pre_upsert_batch_check(entities, fields_info)
        tag = partition_name if isinstance(partition_name, str) else ""
        request = milvus_types.UpsertRequest(collection_name=collection_name, partition_name=tag)

        return cls._parse_batch_request(request, entities, fields_info, location)

    @classmethod
    def delete_request(
        cls,
        collection_name: str,
        partition_name: str,
        expr: str,
        consistency_level: Optional[Union[int, str]],
        **kwargs,
    ):
        def check_str(instr: str, prefix: str):
            if instr is None:
                raise ParamError(message=f"{prefix} cannot be None")
            if not isinstance(instr, str):
                raise ParamError(message=f"{prefix} value {instr} is illegal")
            if len(instr) == 0:
                raise ParamError(message=f"{prefix} cannot be empty")

        check_str(collection_name, "collection_name")
        if partition_name is not None and partition_name != "":
            check_str(partition_name, "partition_name")
        param_name = kwargs.get("param_name", "expr")
        check_str(expr, param_name)

        return milvus_types.DeleteRequest(
            collection_name=collection_name,
            partition_name=partition_name,
            expr=expr,
            consistency_level=get_consistency_level(consistency_level),
        )

    @classmethod
    def _prepare_placeholder_str(cls, data: Any):
        # sparse vector
        if entity_helper.entity_is_sparse_matrix(data):
            pl_type = PlaceholderType.SparseFloatVector
            pl_values = entity_helper.sparse_rows_to_proto(data).contents

        elif isinstance(data[0], np.ndarray):
            dtype = data[0].dtype

            if dtype == "bfloat16":
                pl_type = PlaceholderType.BFLOAT16_VECTOR
                pl_values = (array.tobytes() for array in data)
            elif dtype == "float16":
                pl_type = PlaceholderType.FLOAT16_VECTOR
                pl_values = (array.tobytes() for array in data)
            elif dtype in ("float32", "float64"):
                pl_type = PlaceholderType.FloatVector
                pl_values = (blob.vector_float_to_bytes(entity) for entity in data)

            elif dtype == "byte":
                pl_type = PlaceholderType.BinaryVector
                pl_values = data

            else:
                err_msg = f"unsupported data type: {dtype}"
                raise ParamError(message=err_msg)

        elif isinstance(data[0], bytes):
            pl_type = PlaceholderType.BinaryVector
            pl_values = data  # data is already a list of bytes

        else:
            pl_type = PlaceholderType.FloatVector
            pl_values = (blob.vector_float_to_bytes(entity) for entity in data)

        pl = common_types.PlaceholderValue(tag="$0", type=pl_type, values=pl_values)
        return common_types.PlaceholderGroup.SerializeToString(
            common_types.PlaceholderGroup(placeholders=[pl])
        )

    @classmethod
    def search_requests_with_expr(
        cls,
        collection_name: str,
        data: Union[List, utils.SparseMatrixInputType],
        anns_field: str,
        param: Dict,
        limit: int,
        expr: Optional[str] = None,
        partition_names: Optional[List[str]] = None,
        output_fields: Optional[List[str]] = None,
        round_decimal: int = -1,
        **kwargs,
    ) -> milvus_types.SearchRequest:
        use_default_consistency = ts_utils.construct_guarantee_ts(collection_name, kwargs)

        ignore_growing = param.get("ignore_growing", False) or kwargs.get("ignore_growing", False)
        params = param.get("params", {})
        if not isinstance(params, dict):
            raise ParamError(message=f"Search params must be a dict, got {type(params)}")

        if PAGE_RETAIN_ORDER_FIELD in kwargs and PAGE_RETAIN_ORDER_FIELD in param:
            raise ParamError(
                message="Provide page_retain_order both in kwargs and param, expect just one"
            )
        page_retain_order = kwargs.get(PAGE_RETAIN_ORDER_FIELD) or param.get(
            PAGE_RETAIN_ORDER_FIELD
        )
        if page_retain_order is not None:
            if not isinstance(page_retain_order, bool):
                raise ParamError(
                    message=f"wrong type for page_retain_order, expect bool, got {type(page_retain_order)}"
                )
            params[PAGE_RETAIN_ORDER_FIELD] = page_retain_order

        search_params = {
            "topk": limit,
            "params": params,
            "round_decimal": round_decimal,
            "ignore_growing": ignore_growing,
        }

        # parse offset
        if "offset" in kwargs and "offset" in param:
            raise ParamError(message="Provide offset both in kwargs and param, expect just one")

        offset = kwargs.get("offset") or param.get("offset")
        if offset is not None:
            if not isinstance(offset, int):
                raise ParamError(message=f"wrong type for offset, expect int, got {type(offset)}")
            search_params["offset"] = offset

        is_iterator = kwargs.get(ITERATOR_FIELD)
        if is_iterator is not None:
            search_params[ITERATOR_FIELD] = is_iterator

        group_by_field = kwargs.get(GROUP_BY_FIELD)
        if group_by_field is not None:
            search_params[GROUP_BY_FIELD] = group_by_field

        group_size = kwargs.get(GROUP_SIZE)
        if group_size is not None:
            search_params[GROUP_SIZE] = group_size

        group_strict_size = kwargs.get(GROUP_STRICT_SIZE)
        if group_strict_size is not None:
            search_params[GROUP_STRICT_SIZE] = group_strict_size

        if param.get("metric_type") is not None:
            search_params["metric_type"] = param["metric_type"]

        if anns_field:
            search_params["anns_field"] = anns_field

        req_params = [
            common_types.KeyValuePair(key=str(key), value=utils.dumps(value))
            for key, value in search_params.items()
        ]
        nq = entity_helper.get_input_num_rows(data)
        plg_str = cls._prepare_placeholder_str(data)

        request = milvus_types.SearchRequest(
            collection_name=collection_name,
            partition_names=partition_names,
            output_fields=output_fields,
            guarantee_timestamp=kwargs.get("guarantee_timestamp", 0),
            use_default_consistency=use_default_consistency,
            consistency_level=kwargs.get("consistency_level", 0),
            nq=nq,
            placeholder_group=plg_str,
            dsl_type=common_types.DslType.BoolExprV1,
            search_params=req_params,
        )
        if expr is not None:
            request.dsl = expr

        return request

    @classmethod
    def hybrid_search_request_with_ranker(
        cls,
        collection_name: str,
        reqs: List,
        rerank_param: Dict,
        limit: int,
        partition_names: Optional[List[str]] = None,
        output_fields: Optional[List[str]] = None,
        round_decimal: int = -1,
        **kwargs,
    ) -> milvus_types.HybridSearchRequest:
        use_default_consistency = ts_utils.construct_guarantee_ts(collection_name, kwargs)
        rerank_param["limit"] = limit
        rerank_param["round_decimal"] = round_decimal
        rerank_param["offset"] = kwargs.get("offset", 0)

        request = milvus_types.HybridSearchRequest(
            collection_name=collection_name,
            partition_names=partition_names,
            requests=reqs,
            output_fields=output_fields,
            guarantee_timestamp=kwargs.get("guarantee_timestamp", 0),
            use_default_consistency=use_default_consistency,
            consistency_level=kwargs.get("consistency_level", 0),
        )

        request.rank_params.extend(
            [
                common_types.KeyValuePair(key=str(key), value=utils.dumps(value))
                for key, value in rerank_param.items()
            ]
        )

        group_scorer = kwargs.get(RANK_GROUP_SCORER, "max")
        request.rank_params.extend(
            [common_types.KeyValuePair(key=RANK_GROUP_SCORER, value=group_scorer)]
        )
        return request

    @classmethod
    def create_alias_request(cls, collection_name: str, alias: str):
        return milvus_types.CreateAliasRequest(collection_name=collection_name, alias=alias)

    @classmethod
    def drop_alias_request(cls, alias: str):
        return milvus_types.DropAliasRequest(alias=alias)

    @classmethod
    def alter_alias_request(cls, collection_name: str, alias: str):
        return milvus_types.AlterAliasRequest(collection_name=collection_name, alias=alias)

    @classmethod
    def describe_alias_request(cls, alias: str):
        return milvus_types.DescribeAliasRequest(alias=alias)

    @classmethod
    def list_aliases_request(cls, collection_name: str, db_name: str = ""):
        return milvus_types.ListAliasesRequest(collection_name=collection_name, db_name=db_name)

    @classmethod
    def create_index_request(cls, collection_name: str, field_name: str, params: Dict, **kwargs):
        index_params = milvus_types.CreateIndexRequest(
            collection_name=collection_name,
            field_name=field_name,
            index_name=kwargs.get("index_name", ""),
        )

        if isinstance(params, dict):
            for tk, tv in params.items():
                if tk == "dim" and (not tv or not isinstance(tv, int)):
                    raise ParamError(message="dim must be of int!")
                kv_pair = common_types.KeyValuePair(key=str(tk), value=utils.dumps(tv))
                index_params.extra_params.append(kv_pair)

        return index_params

    @classmethod
    def alter_index_request(cls, collection_name: str, index_name: str, extra_params: dict):
        params = []
        for k, v in extra_params.items():
            params.append(common_types.KeyValuePair(key=str(k), value=utils.dumps(v)))
        return milvus_types.AlterIndexRequest(
            collection_name=collection_name, index_name=index_name, extra_params=params
        )

    @classmethod
    def describe_index_request(
        cls, collection_name: str, index_name: str, timestamp: Optional[int] = None
    ):
        return milvus_types.DescribeIndexRequest(
            collection_name=collection_name, index_name=index_name, timestamp=timestamp
        )

    @classmethod
    def get_index_build_progress(cls, collection_name: str, index_name: str):
        return milvus_types.GetIndexBuildProgressRequest(
            collection_name=collection_name, index_name=index_name
        )

    @classmethod
    def get_index_state_request(cls, collection_name: str, index_name: str):
        return milvus_types.GetIndexStateRequest(
            collection_name=collection_name, index_name=index_name
        )

    @classmethod
    def load_collection(
        cls,
        db_name: str,
        collection_name: str,
        replica_number: int,
        refresh: bool,
        resource_groups: List[str],
        load_fields: List[str],
        skip_load_dynamic_field: bool,
    ):
        return milvus_types.LoadCollectionRequest(
            db_name=db_name,
            collection_name=collection_name,
            replica_number=replica_number,
            refresh=refresh,
            resource_groups=resource_groups,
            load_fields=load_fields,
            skip_load_dynamic_field=skip_load_dynamic_field,
        )

    @classmethod
    def release_collection(cls, db_name: str, collection_name: str):
        return milvus_types.ReleaseCollectionRequest(
            db_name=db_name, collection_name=collection_name
        )

    @classmethod
    def load_partitions(
        cls,
        db_name: str,
        collection_name: str,
        partition_names: List[str],
        replica_number: int,
        refresh: bool,
        resource_groups: List[str],
        load_fields: List[str],
        skip_load_dynamic_field: bool,
    ):
        return milvus_types.LoadPartitionsRequest(
            db_name=db_name,
            collection_name=collection_name,
            partition_names=partition_names,
            replica_number=replica_number,
            refresh=refresh,
            resource_groups=resource_groups,
            load_fields=load_fields,
            skip_load_dynamic_field=skip_load_dynamic_field,
        )

    @classmethod
    def release_partitions(cls, db_name: str, collection_name: str, partition_names: List[str]):
        return milvus_types.ReleasePartitionsRequest(
            db_name=db_name, collection_name=collection_name, partition_names=partition_names
        )

    @classmethod
    def get_collection_stats_request(cls, collection_name: str):
        return milvus_types.GetCollectionStatisticsRequest(collection_name=collection_name)

    @classmethod
    def get_persistent_segment_info_request(cls, collection_name: str):
        return milvus_types.GetPersistentSegmentInfoRequest(collectionName=collection_name)

    @classmethod
    def get_flush_state_request(cls, segment_ids: List[int], collection_name: str, flush_ts: int):
        return milvus_types.GetFlushStateRequest(
            segmentIDs=segment_ids, collection_name=collection_name, flush_ts=flush_ts
        )

    @classmethod
    def get_query_segment_info_request(cls, collection_name: str):
        return milvus_types.GetQuerySegmentInfoRequest(collectionName=collection_name)

    @classmethod
    def flush_param(cls, collection_names: List[str]):
        return milvus_types.FlushRequest(collection_names=collection_names)

    @classmethod
    def drop_index_request(cls, collection_name: str, field_name: str, index_name: str):
        return milvus_types.DropIndexRequest(
            db_name="",
            collection_name=collection_name,
            field_name=field_name,
            index_name=index_name,
        )

    @classmethod
    def get_partition_stats_request(cls, collection_name: str, partition_name: str):
        return milvus_types.GetPartitionStatisticsRequest(
            db_name="", collection_name=collection_name, partition_name=partition_name
        )

    @classmethod
    def dummy_request(cls, request_type: Any):
        return milvus_types.DummyRequest(request_type=request_type)

    @classmethod
    def retrieve_request(
        cls,
        collection_name: str,
        ids: List[str],
        output_fields: List[str],
        partition_names: List[str],
    ):
        ids = schema_types.IDs(int_id=schema_types.LongArray(data=ids))
        return milvus_types.RetrieveRequest(
            db_name="",
            collection_name=collection_name,
            ids=ids,
            output_fields=output_fields,
            partition_names=partition_names,
        )

    @classmethod
    def query_request(
        cls,
        collection_name: str,
        expr: str,
        output_fields: List[str],
        partition_names: List[str],
        **kwargs,
    ):
        use_default_consistency = ts_utils.construct_guarantee_ts(collection_name, kwargs)
        req = milvus_types.QueryRequest(
            db_name="",
            collection_name=collection_name,
            expr=expr,
            output_fields=output_fields,
            partition_names=partition_names,
            guarantee_timestamp=kwargs.get("guarantee_timestamp", 0),
            use_default_consistency=use_default_consistency,
            consistency_level=kwargs.get("consistency_level", 0),
        )

        limit = kwargs.get("limit", None)
        if limit is not None:
            req.query_params.append(common_types.KeyValuePair(key="limit", value=str(limit)))

        offset = kwargs.get("offset", None)
        if offset is not None:
            req.query_params.append(common_types.KeyValuePair(key="offset", value=str(offset)))

        ignore_growing = kwargs.get("ignore_growing", False)
        stop_reduce_for_best = kwargs.get(REDUCE_STOP_FOR_BEST, False)
        is_iterator = kwargs.get(ITERATOR_FIELD)
        if is_iterator is not None:
            req.query_params.append(
                common_types.KeyValuePair(key=ITERATOR_FIELD, value=is_iterator)
            )

        req.query_params.append(
            common_types.KeyValuePair(key="ignore_growing", value=str(ignore_growing))
        )
        req.query_params.append(
            common_types.KeyValuePair(key=REDUCE_STOP_FOR_BEST, value=str(stop_reduce_for_best))
        )
        return req

    @classmethod
    def load_balance_request(
        cls,
        collection_name: str,
        src_node_id: int,
        dst_node_ids: List[int],
        sealed_segment_ids: List[int],
    ):
        return milvus_types.LoadBalanceRequest(
            collectionName=collection_name,
            src_nodeID=src_node_id,
            dst_nodeIDs=dst_node_ids,
            sealed_segmentIDs=sealed_segment_ids,
        )

    @classmethod
    def manual_compaction(cls, collection_id: int, is_clustering: bool):
        if collection_id is None or not isinstance(collection_id, int):
            raise ParamError(message=f"collection_id value {collection_id} is illegal")

        if is_clustering is None or not isinstance(is_clustering, bool):
            raise ParamError(message=f"is_clustering value {is_clustering} is illegal")

        request = milvus_types.ManualCompactionRequest()
        request.collectionID = collection_id
        request.majorCompaction = is_clustering

        return request

    @classmethod
    def get_compaction_state(cls, compaction_id: int):
        if compaction_id is None or not isinstance(compaction_id, int):
            raise ParamError(message=f"compaction_id value {compaction_id} is illegal")

        request = milvus_types.GetCompactionStateRequest()
        request.compactionID = compaction_id
        return request

    @classmethod
    def get_compaction_state_with_plans(cls, compaction_id: int):
        if compaction_id is None or not isinstance(compaction_id, int):
            raise ParamError(message=f"compaction_id value {compaction_id} is illegal")

        request = milvus_types.GetCompactionPlansRequest()
        request.compactionID = compaction_id
        return request

    @classmethod
    def get_replicas(cls, collection_id: int):
        if collection_id is None or not isinstance(collection_id, int):
            raise ParamError(message=f"collection_id value {collection_id} is illegal")

        return milvus_types.GetReplicasRequest(
            collectionID=collection_id,
            with_shard_nodes=True,
        )

    @classmethod
    def do_bulk_insert(cls, collection_name: str, partition_name: str, files: list, **kwargs):
        channel_names = kwargs.get("channel_names", None)
        req = milvus_types.ImportRequest(
            collection_name=collection_name,
            partition_name=partition_name,
            files=files,
        )
        if channel_names is not None:
            req.channel_names.extend(channel_names)

        for k, v in kwargs.items():
            if k in ("bucket", "backup"):
                kv_pair = common_types.KeyValuePair(key=str(k), value=str(v))
                req.options.append(kv_pair)

        return req

    @classmethod
    def get_bulk_insert_state(cls, task_id: int):
        if task_id is None or not isinstance(task_id, int):
            msg = f"task_id value {task_id} is not an integer"
            raise ParamError(msg)

        return milvus_types.GetImportStateRequest(task=task_id)

    @classmethod
    def list_bulk_insert_tasks(cls, limit: int, collection_name: str):
        if limit is None or not isinstance(limit, int):
            msg = f"limit value {limit} is not an integer"
            raise ParamError(msg)

        return milvus_types.ListImportTasksRequest(
            collection_name=collection_name,
            limit=limit,
        )

    @classmethod
    def create_user_request(cls, user: str, password: str):
        check_pass_param(user=user, password=password)
        return milvus_types.CreateCredentialRequest(
            username=user, password=base64.b64encode(password.encode("utf-8"))
        )

    @classmethod
    def update_password_request(cls, user: str, old_password: str, new_password: str):
        check_pass_param(user=user)
        check_pass_param(password=old_password)
        check_pass_param(password=new_password)
        return milvus_types.UpdateCredentialRequest(
            username=user,
            oldPassword=base64.b64encode(old_password.encode("utf-8")),
            newPassword=base64.b64encode(new_password.encode("utf-8")),
        )

    @classmethod
    def delete_user_request(cls, user: str):
        if not isinstance(user, str):
            raise ParamError(message=f"invalid user {user}")
        return milvus_types.DeleteCredentialRequest(username=user)

    @classmethod
    def list_usernames_request(cls):
        return milvus_types.ListCredUsersRequest()

    @classmethod
    def create_role_request(cls, role_name: str):
        check_pass_param(role_name=role_name)
        return milvus_types.CreateRoleRequest(entity=milvus_types.RoleEntity(name=role_name))

    @classmethod
    def drop_role_request(cls, role_name: str):
        check_pass_param(role_name=role_name)
        return milvus_types.DropRoleRequest(role_name=role_name)

    @classmethod
    def operate_user_role_request(cls, username: str, role_name: str, operate_user_role_type: Any):
        check_pass_param(user=username)
        check_pass_param(role_name=role_name)
        check_pass_param(operate_user_role_type=operate_user_role_type)
        return milvus_types.OperateUserRoleRequest(
            username=username, role_name=role_name, type=operate_user_role_type
        )

    @classmethod
    def select_role_request(cls, role_name: str, include_user_info: bool):
        if role_name:
            check_pass_param(role_name=role_name)
        check_pass_param(include_user_info=include_user_info)
        return milvus_types.SelectRoleRequest(
            role=milvus_types.RoleEntity(name=role_name) if role_name else None,
            include_user_info=include_user_info,
        )

    @classmethod
    def select_user_request(cls, username: str, include_role_info: bool):
        if username:
            check_pass_param(user=username)
        check_pass_param(include_role_info=include_role_info)
        return milvus_types.SelectUserRequest(
            user=milvus_types.UserEntity(name=username) if username else None,
            include_role_info=include_role_info,
        )

    @classmethod
    def operate_privilege_request(
        cls,
        role_name: str,
        object: Any,
        object_name: str,
        privilege: str,
        db_name: str,
        operate_privilege_type: Any,
    ):
        check_pass_param(role_name=role_name)
        check_pass_param(object=object)
        check_pass_param(object_name=object_name)
        check_pass_param(privilege=privilege)
        check_pass_param(operate_privilege_type=operate_privilege_type)
        return milvus_types.OperatePrivilegeRequest(
            entity=milvus_types.GrantEntity(
                role=milvus_types.RoleEntity(name=role_name),
                object=milvus_types.ObjectEntity(name=object),
                object_name=object_name,
                db_name=db_name,
                grantor=milvus_types.GrantorEntity(
                    privilege=milvus_types.PrivilegeEntity(name=privilege)
                ),
            ),
            type=operate_privilege_type,
        )

    @classmethod
    def select_grant_request(cls, role_name: str, object: str, object_name: str, db_name: str):
        check_pass_param(role_name=role_name)
        if object:
            check_pass_param(object=object)
        if object_name:
            check_pass_param(object_name=object_name)
        return milvus_types.SelectGrantRequest(
            entity=milvus_types.GrantEntity(
                role=milvus_types.RoleEntity(name=role_name),
                object=milvus_types.ObjectEntity(name=object) if object else None,
                object_name=object_name if object_name else None,
                db_name=db_name,
            ),
        )

    @classmethod
    def get_server_version(cls):
        return milvus_types.GetVersionRequest()

    @classmethod
    def create_resource_group(cls, name: str, **kwargs):
        check_pass_param(resource_group_name=name)
        return milvus_types.CreateResourceGroupRequest(
            resource_group=name,
            config=kwargs.get("config"),
        )

    @classmethod
    def update_resource_groups(cls, configs: Mapping[str, ResourceGroupConfig]):
        return milvus_types.UpdateResourceGroupsRequest(
            resource_groups=configs,
        )

    @classmethod
    def drop_resource_group(cls, name: str):
        check_pass_param(resource_group_name=name)
        return milvus_types.DropResourceGroupRequest(resource_group=name)

    @classmethod
    def list_resource_groups(cls):
        return milvus_types.ListResourceGroupsRequest()

    @classmethod
    def describe_resource_group(cls, name: str):
        check_pass_param(resource_group_name=name)
        return milvus_types.DescribeResourceGroupRequest(resource_group=name)

    @classmethod
    def transfer_node(cls, source: str, target: str, num_node: int):
        check_pass_param(resource_group_name=source)
        check_pass_param(resource_group_name=target)
        return milvus_types.TransferNodeRequest(
            source_resource_group=source, target_resource_group=target, num_node=num_node
        )

    @classmethod
    def transfer_replica(cls, source: str, target: str, collection_name: str, num_replica: int):
        check_pass_param(resource_group_name=source)
        check_pass_param(resource_group_name=target)
        return milvus_types.TransferReplicaRequest(
            source_resource_group=source,
            target_resource_group=target,
            collection_name=collection_name,
            num_replica=num_replica,
        )

    @classmethod
    def flush_all_request(cls, db_name: str):
        return milvus_types.FlushAllRequest(db_name=db_name)

    @classmethod
    def get_flush_all_state_request(cls, flush_all_ts: int, db_name: str):
        return milvus_types.GetFlushAllStateRequest(flush_all_ts=flush_all_ts, db_name=db_name)

    @classmethod
    def register_request(cls, user: str, host: str, **kwargs):
        reserved = {}
        for k, v in kwargs.items():
            reserved[k] = v
        now = datetime.datetime.now()
        this = common_types.ClientInfo(
            sdk_type="Python",
            sdk_version=__version__,
            local_time=now.__str__(),
            reserved=reserved,
        )
        if user is not None:
            this.user = user
        if host is not None:
            this.host = host
        return milvus_types.ConnectRequest(
            client_info=this,
        )

    @classmethod
    def create_database_req(cls, db_name: str, **kwargs):
        check_pass_param(db_name=db_name)

        req = milvus_types.CreateDatabaseRequest(db_name=db_name)
        properties = kwargs.get("properties")
        if is_legal_collection_properties(properties):
            properties = [
                common_types.KeyValuePair(key=str(k), value=str(v)) for k, v in properties.items()
            ]
            req.properties.extend(properties)
        return req

    @classmethod
    def drop_database_req(cls, db_name: str):
        check_pass_param(db_name=db_name)
        return milvus_types.DropDatabaseRequest(db_name=db_name)

    @classmethod
    def list_database_req(cls):
        return milvus_types.ListDatabasesRequest()

    @classmethod
    def alter_database_req(cls, db_name: str, properties: Dict):
        check_pass_param(db_name=db_name)
        kvs = [common_types.KeyValuePair(key=k, value=str(v)) for k, v in properties.items()]
        return milvus_types.AlterDatabaseRequest(db_name=db_name, properties=kvs)

    @classmethod
    def describe_database_req(cls, db_name: str):
        check_pass_param(db_name=db_name)
        return milvus_types.DescribeDatabaseRequest(db_name=db_name)
