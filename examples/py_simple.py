from slingshot_microservice.typing import ReadFileFn, WriteFileFn
from slingshot_microservice import Microservice
from typing import Generator

def process(
	request: int,
	read_file: ReadFileFn,
	write_file: WriteFileFn,
) -> Generator[tuple[int, bool | int | str], None, None]:
	reader = read_file("in", request)
	input_data = reader.read().decode()
	writer = write_file("out", request)
	writer.write(f"Hello {input_data}".encode())
	yield (request, True)


microservice = Microservice("simple-py-microservice", "sys-map.slingshot.cv", process)
microservice.start()
