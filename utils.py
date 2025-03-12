import uuid
import string
import json
import decimal


MAP = {
    1: "varchar",
    2: "uuid",
    3: "char",
    4: "numeric",
    5: "json",
}

def check_types(lst, type_map=None):
    if type_map is None:
        type_map = {
            str: lambda x: 2 if is_uuid(x) else (3 if is_char(x) else 1),  # UUID -> 2, Char -> 3, String -> 1
            (float, decimal.Decimal): lambda x: 4,  # Numeric -> 4
            dict: lambda x: 5 if is_json(x) else 0,  # JSON -> 5
        }

    def is_uuid(value):
        try:
            uuid.UUID(value)
            return True
        except ValueError:
            return False

    def is_char(value):
        return len(value) == 1 and value in string.printable  # Kiểm tra ký tự đơn hợp lệ

    def is_json(value):
        try:
            json.dumps(value)  # Kiểm tra xem có thể serialize thành JSON không
            return True
        except (TypeError, ValueError):
            return False

    def get_type(item):
        for types, handler in type_map.items():
            if isinstance(item, types if isinstance(types, tuple) else (types,)):
                return handler(item)
        return 0

    return [get_type(item) for item in lst]
