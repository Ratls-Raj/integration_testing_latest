from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
import re

deserializer = TypeDeserializer()
serializer = TypeSerializer()


class MockDatabase:
    # Convert DynamoDB client format → Python
    def _deserialize_item(self, item):
        new_item = {}
        for k, v in item.items():
            if isinstance(v, dict) and len(v) == 1:
                new_item[k] = deserializer.deserialize(v)
            else:
                new_item[k] = v
        return new_item

    # Convert Python dict → DynamoDB client format
    def _serialize_item(self, item):
        new_item = {}

        for k, v in item.items():

            # already DynamoDB format
            if isinstance(v, dict) and len(v) == 1 and list(v.keys())[0] in [
                "S", "N", "M", "L", "BOOL", "NULL", "B", "SS", "NS", "BS"
            ]:
                new_item[k] = v

            else:
                new_item[k] = serializer.serialize(v)

        return new_item

    def __init__(self):
        self.collections = {}

    def create_collection(self, name):
        if name not in self.collections:
            self.collections[name] = []

    def insert(self, collection_name, records):
        if collection_name not in self.collections:
            self.collections[collection_name] = []

        if isinstance(records, list):
            for record in records:
                serialized = self._serialize_item(record)
                self.collections[collection_name].append(serialized)
        else:
            serialized = self._serialize_item(records)
            self.collections[collection_name].append(serialized)

    def fetch_all(self, collection_name):
        return self.collections.get(collection_name, [])
    
    def query(self, **kwargs):
        print("\n========== MOCK DYNAMODB QUERY START ==========")

        print("Query received:", kwargs)

        table_name = kwargs.get("TableName")
        print("TableName:", table_name)

        data = self.collections.get(table_name, [])
        print("Total records in table:", len(data))

        values = kwargs.get("ExpressionAttributeValues", {})
        print("Raw ExpressionAttributeValues:", values)

        # Deserialize query values
        query_vals = {}
        for k, v in values.items():
            query_vals[k] = deserializer.deserialize(v)

        print("Deserialized query values:", query_vals)

        expression_names = kwargs.get("ExpressionAttributeNames", {})
        key_expression = kwargs.get("KeyConditionExpression", "")
        filter_expression = kwargs.get("FilterExpression", "")
        limit = kwargs.get("Limit")
        scan_index_forward = kwargs.get("ScanIndexForward", True)

        print("KeyConditionExpression:", key_expression)
        print("FilterExpression:", filter_expression)
        print("ExpressionAttributeNames:", expression_names)
        print("ScanIndexForward:", scan_index_forward)

        results = []

        print("\n--- Scanning records ---")

        def _resolve_attr(token):
            token = token.strip()
            if token in expression_names:
                return expression_names[token]
            return token.replace("#", "")

        def _resolve_value(token):
            token = token.strip()
            return query_vals.get(token)

        def _eval_clause(item_py, clause):
            clause = clause.strip()
            if not clause:
                return True

            begins_with_match = re.match(
                r"begins_with\((#[A-Za-z0-9_]+)\s*,\s*(:[A-Za-z0-9_]+)\)",
                clause
            )
            if begins_with_match:
                attr_token, value_token = begins_with_match.groups()
                attr_name = _resolve_attr(attr_token)
                expected = _resolve_value(value_token)
                actual = item_py.get(attr_name)
                return isinstance(actual, str) and str(actual).startswith(str(expected))

            if "=" in clause:
                lhs, rhs = [part.strip() for part in clause.split("=", 1)]
                attr_name = _resolve_attr(lhs)
                expected = _resolve_value(rhs)
                return item_py.get(attr_name) == expected

            # If we don't recognize the clause, do not fail closed in test infra.
            return True

        def _eval_expression(item_py, expression):
            if not expression:
                return True
            clauses = [part.strip() for part in expression.split("And")]
            return all(_eval_clause(item_py, clause) for clause in clauses)

        for item in data:

            print("\nChecking item:", item)

            item_py = self._deserialize_item(item)
            print("Deserialized item:", item_py)

            key_ok = _eval_expression(item_py, key_expression)
            filter_ok = _eval_expression(item_py, filter_expression)

            print("Key match:", key_ok)
            print("Filter match:", filter_ok)

            if key_ok and filter_ok:
                print("MATCH FOUND")
                results.append(item)
            else:
                print("No match")

        # Sort if query uses lastContacted-like fields and order was requested.
        sort_attr = None
        begins_with_attrs = re.findall(r"begins_with\((#[A-Za-z0-9_]+)", key_expression)
        if begins_with_attrs:
            sort_attr = _resolve_attr(begins_with_attrs[0])
        if sort_attr:
            try:
                results.sort(
                    key=lambda x: str(self._deserialize_item(x).get(sort_attr, "")),
                    reverse=not scan_index_forward
                )
            except Exception:
                pass

        if limit and isinstance(limit, int):
            results = results[:limit]

        print("\nTotal results returned:", len(results))

        print("========== MOCK DYNAMODB QUERY END ==========\n")

        return {"Items": results}

    def reset(self):
        self.collections = {}
