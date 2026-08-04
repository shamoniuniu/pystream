"""对象存储端口的稳定错误分类。"""


class ObjectStoreError(RuntimeError):
    """对象存储操作失败。"""


class ObjectNotFound(ObjectStoreError):
    """目标对象不存在。"""


class ObjectConflict(ObjectStoreError):
    """条件写入因对象已存在或 ETag 过期而失败。"""


__all__ = ["ObjectConflict", "ObjectNotFound", "ObjectStoreError"]
