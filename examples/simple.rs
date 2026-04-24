use slingshot_microservice::{AnyError, Microservice, ReadFileFn, WriteFileFn};
use std::io::{Read, Write};

fn process(
	request: u64,
	read_file: &ReadFileFn,
	write_file: &WriteFileFn,
) -> Result<Vec<(u64, String)>, AnyError> {
	let mut input = String::new();
	let mut reader = read_file("in", request)?;
	reader.read_to_string(&mut input)?;

	let mut writer = write_file("out", request)?;
	writer.write_all(input.as_bytes())?;

	Ok(vec![(request, "case_a".to_string())])
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let microservice = Microservice::new("simple-microservice", "sys-map.slingshot.cv", process);

	microservice.start()?;
	Ok(())
}
