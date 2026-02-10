import numpy as np
from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
)
from pymilvus.milvus_client import MilvusClient
from datetime import datetime, timezone
import random

names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace"]
categories = ["Electronics", "Books", "Clothing", "Food", "Sports"]
collection_name = 'test_query_order_by'
clean_exist = True
prepare_data = True
to_flush = True
batch_num = 3
num_entities, dim = 100, 8
fmt = "\n=== {:30} ===\n"

print(fmt.format("start connecting to Milvus"))
client = MilvusClient(uri="http://localhost:19530")

if clean_exist and client.has_collection(collection_name):
    client.drop_collection(collection_name)

TS = "ts"
fields = [
    FieldSchema(name="pk", dtype=DataType.INT64, is_primary=True, auto_id=True),
    FieldSchema(name="name", dtype=DataType.VARCHAR, max_length=100),
    FieldSchema(name="category", dtype=DataType.VARCHAR, max_length=100),
    FieldSchema(name="price", dtype=DataType.DOUBLE),
    FieldSchema(name="rating", dtype=DataType.INT32),
    FieldSchema(name="stock", dtype=DataType.INT64),
    FieldSchema(name=TS, dtype=DataType.TIMESTAMPTZ, description="timestamp with timezone"),
    FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=dim),
]

schema = CollectionSchema(fields)

print(fmt.format(f"Create collection `{collection_name}`"))
client.create_collection(
    collection_name=collection_name,
    schema=schema,
    consistency_level="Strong"
)

if prepare_data:
    rng = np.random.default_rng(seed=19530)
    print(fmt.format("Start inserting entities"))

    ts_choices = [
        datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc).isoformat(),
        datetime(2025, 1, 2, 0, 0, 0, tzinfo=timezone.utc).isoformat(),
        datetime(2025, 1, 3, 0, 0, 0, tzinfo=timezone.utc).isoformat(),
    ]

    for batch_idx in range(batch_num):
        name_data = [random.choice(names) for _ in range(num_entities)]
        category_data = [random.choice(categories) for _ in range(num_entities)]
        price_data = [round(random.uniform(10.0, 500.0), 2) for _ in range(num_entities)]
        rating_data = [random.randint(1, 5) for _ in range(num_entities)]
        stock_data = [random.randint(0, 1000) for _ in range(num_entities)]
        ts_data = [ts_choices[(batch_idx + j) % len(ts_choices)] for j in range(num_entities)]
        vector_data = rng.random((num_entities, dim))

        data = [
            {
                "name": name_data[i],
                "category": category_data[i],
                "price": price_data[i],
                "rating": rating_data[i],
                "stock": stock_data[i],
                TS: ts_data[i],
                "embedding": vector_data[i].tolist(),
            }
            for i in range(num_entities)
        ]

        client.insert(collection_name, data)
        if to_flush:
            print(f"flush batch:{batch_idx}")
            client.flush(collection_name)
        print(f"inserted batch:{batch_idx}")

    print(fmt.format("Start Creating index IVF_FLAT"))
    from pymilvus.milvus_client.index import IndexParams
    index_params = IndexParams()
    index_params.add_index("embedding", index_type="IVF_FLAT", metric_type="L2", nlist=128)
    client.create_index(collection_name, index_params)

stats = client.get_collection_stats(collection_name)
print(f"Number of entities in Milvus: {stats.get('row_count', 0)}")
client.load_collection(collection_name)


# ==============================================================================
# Query Order By Examples
# ==============================================================================

# 1. Basic ORDER BY: Sort by price ascending (lowest prices first)
print(fmt.format("Query: ORDER BY price ASC"))
res = client.query(
    collection_name=collection_name,
    filter="price > 50",
    output_fields=["name", "category", "price"],
    limit=10,
    order_by=["price:asc"],
)
for row in res:
    print(f"  name={row['name']}, category={row['category']}, price={row['price']}")


# # 2. ORDER BY descending: Sort by price descending (highest prices first)
# print(fmt.format("Query: ORDER BY price DESC"))
# res = client.query(
#     collection_name=collection_name,
#     filter="price > 50",
#     output_fields=["name", "category", "price"],
#     limit=10,
#     order_by=["price:desc"],
# )
# for row in res:
#     print(f"  name={row['name']}, category={row['category']}, price={row['price']}")
#
#
# # 3. Multi-field ORDER BY: Sort by rating DESC, then by price ASC
# print(fmt.format("Query: ORDER BY rating DESC, price ASC"))
# res = client.query(
#     collection_name=collection_name,
#     filter="stock > 100",
#     output_fields=["name", "rating", "price"],
#     limit=15,
#     order_by=["rating:desc", "price:asc"],
# )
# for row in res:
#     print(f"  name={row['name']}, rating={row['rating']}, price={row['price']}")
#
#
# # 4. ORDER BY with string field: Sort by category alphabetically
# print(fmt.format("Query: ORDER BY category ASC"))
# res = client.query(
#     collection_name=collection_name,
#     filter="rating >= 3",
#     output_fields=["name", "category", "rating"],
#     limit=10,
#     order_by=["category:asc"],
# )
# for row in res:
#     print(f"  name={row['name']}, category={row['category']}, rating={row['rating']}")
#
#
# # 5. ORDER BY with timestamp: Sort by most recent first
# print(fmt.format("Query: ORDER BY timestamp DESC"))
# res = client.query(
#     collection_name=collection_name,
#     filter="price > 100",
#     output_fields=["name", "price", TS],
#     limit=10,
#     order_by=[f"{TS}:desc"],
# )
# for row in res:
#     print(f"  name={row['name']}, price={row['price']}, ts={row[TS]}")
#
#
# # 6. ORDER BY with integer field: Sort by stock quantity
# print(fmt.format("Query: ORDER BY stock DESC"))
# res = client.query(
#     collection_name=collection_name,
#     filter="category == 'Electronics'",
#     output_fields=["name", "category", "stock", "price"],
#     limit=10,
#     order_by=["stock:desc"],
# )
# for row in res:
#     print(f"  name={row['name']}, category={row['category']}, stock={row['stock']}, price={row['price']}")
#
#
# # 7. ORDER BY with complex filter: Combine filter expression with ordering
# print(fmt.format("Query: Complex filter + ORDER BY"))
# res = client.query(
#     collection_name=collection_name,
#     filter="price > 100 and rating >= 3 and stock > 50",
#     output_fields=["name", "category", "price", "rating", "stock"],
#     limit=10,
#     order_by=["price:desc"],
# )
# for row in res:
#     print(f"  name={row['name']}, category={row['category']}, price={row['price']}, rating={row['rating']}, stock={row['stock']}")
#
#
# # 8. ORDER BY with GROUP BY + aggregation (Phase 2 feature)
# # Note: This combines GROUP BY aggregation with ORDER BY on aggregate values
# print(fmt.format("Query: GROUP BY + ORDER BY sum(price)"))
# try:
#     res = client.query(
#         collection_name=collection_name,
#         filter="stock > 0",
#         output_fields=["category", "sum(price)", "count(*)"],
#         group_by_fields=["category"],
#         order_by=["sum(price):desc"],
#         limit=10,
#     )
#     for row in res:
#         print(f"  category={row['category']}, sum(price)={row.get('sum(price)', 'N/A')}, count={row.get('count(*)', 'N/A')}")
# except Exception as e:
#     print(f"  Note: GROUP BY + ORDER BY on aggregates may require Phase 2 implementation")
#     print(f"  Error: {e}")
#
#
# # 9. ORDER BY with offset (pagination)
# print(fmt.format("Query: ORDER BY with offset (page 2)"))
# res = client.query(
#     collection_name=collection_name,
#     filter="price > 50",
#     output_fields=["name", "price"],
#     limit=5,
#     offset=5,
#     order_by=["price:asc"],
# )
# print("Page 2 (offset=5, limit=5):")
# for row in res:
#     print(f"  name={row['name']}, price={row['price']}")
#
#
# # 10. ORDER BY without explicit filter (match all)
# print(fmt.format("Query: ORDER BY all records"))
# res = client.query(
#     collection_name=collection_name,
#     filter="",
#     output_fields=["name", "price", "rating"],
#     limit=10,
#     order_by=["rating:desc", "price:desc"],
# )
# for row in res:
#     print(f"  name={row['name']}, rating={row['rating']}, price={row['price']}")


print(fmt.format("Query Order By examples completed"))
