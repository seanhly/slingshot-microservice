use slingshot_microservice::{Microservice, ProcessFuture, ReadFileFn, WriteFileFn};
use std::io::Write;
use tokio::io::AsyncReadExt;

fn process<'a>(
	request: u64,
	read_file: &'a ReadFileFn,
	write_file: &'a WriteFileFn,
) -> ProcessFuture<'a, String> {
	Box::pin(async move {
		let mut input = String::new();
		let mut reader = read_file("in", request)?;
		reader.read_to_string(&mut input).await?;

		let mut writer = write_file("out", request)?;
		writer.write_all(input.as_bytes())?;

		Ok(vec![(request, "case_a".to_string())])
	})
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let microservice = Microservice::new("simple-microservice", "sys-map.slingshot.cv", process);

	microservice.start()?;
	Ok(())
}
