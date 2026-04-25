from __future__ import annotations

from typing import BinaryIO, Generator, Protocol, TypeAlias

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
    ) -> Generator[tuple[int, CaseVariable], None, None]: ...


__all__ = [
    "CaseVariable",
    "ReadFileFn",
    "WriteFileFn",
    "ProcessFn",
]
