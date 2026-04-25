from __future__ import annotations

from typing import TYPE_CHECKING, BinaryIO, Generator, Protocol, TypeAlias

if TYPE_CHECKING:
    from sqlalchemy.engine.base import Connection as SqlAlchemyConnection
else:
    SqlAlchemyConnection = object

CaseVariable: TypeAlias = bool | int | str


class ReadFileFn(Protocol):
    def __call__(self, key: str, id: int) -> BinaryIO: ...


class WriteFileFn(Protocol):
    def __call__(self, key: str, id: int) -> BinaryIO: ...


class ProcessFn(Protocol):
    def __call__(
        self,
        request: int,
        read_file: ReadFileFn,
        write_file: WriteFileFn,
        connection: SqlAlchemyConnection,
    ) -> Generator[tuple[int, CaseVariable], None, None]: ...


__all__ = [
    "CaseVariable",
    "ReadFileFn",
    "WriteFileFn",
    "ProcessFn",
]
