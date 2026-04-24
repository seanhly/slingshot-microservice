use std::collections::HashMap;
use std::error::Error;
use std::process::Command;
use std::sync::Arc;

use futures_util::StreamExt;
use lapin::options::{
	BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
	QueueDeclareOptions,
};
use tracing::{error, info};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Channel, Connection, ConnectionProperties};
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use tracing_subscriber::EnvFilter;

type AnyError = Box<dyn Error + Send + Sync + 'static>;

type ProcessFn = dyn Fn(u64) -> Vec<(u64, Value)> + Send + Sync + 'static;

fn init_tracing() {
	let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
	tracing_subscriber::fmt().with_env_filter(filter).init();
}

#[derive(Debug, Deserialize)]
struct QueueConfig {
	#[serde(rename = "in")]
	inbound: String,
	out: Vec<OutboundCase>,
}

#[derive(Debug, Deserialize)]
struct OutboundCase {
	#[serde(rename = "case")]
	case_value: Value,
	queues: Vec<String>,
}

/// A simple queue-driven microservice runtime.
///
/// The microservice:
/// 1) Retrieves queue metadata from a configuration service,
/// 2) Consumes u64 IDs from an inbound queue,
/// 3) Runs the user-provided processing function,
/// 4) Routes each output ID to outbound queue(s) based on case variables.
pub struct Microservice {
	name: String,
	config_host: String,
	process: Arc<ProcessFn>,
}

impl Microservice {
	/// Create a new microservice runtime.
	///
	/// `process` accepts an inbound request ID and returns a list of
	/// `(result_id, case_variable)` tuples. Case variables can be any
	/// serializable primitive, such as `String`, `bool`, or integers.
	pub fn new<F, C>(name: impl Into<String>, config_host: impl Into<String>, process: F) -> Self
	where
		F: Fn(u64) -> Vec<(u64, C)> + Send + Sync + 'static,
		C: Serialize,
	{
		init_tracing();
		let process_wrapper = move |request: u64| -> Vec<(u64, Value)> {
			process(request)
				.into_iter()
				.map(|(id, case)| {
					let value = serde_json::to_value(case)
						.expect("case variable must be serializable to JSON");
					(id, value)
				})
				.collect()
		};

		Self {
			name: name.into(),
			config_host: config_host.into(),
			process: Arc::new(process_wrapper),
		}
	}

	/// Start the microservice. This call blocks while the consumer loop runs.
	pub fn start(&self) -> Result<(), AnyError> {
		let config = self.fetch_config()?;
		let route_map = build_route_map(&config.out);
		let amqp_url = fetch_rabbitmq_url_from_sys_map()?;

		let runtime = tokio::runtime::Builder::new_multi_thread()
			.enable_all()
			.build()?;

		runtime.block_on(self.run_consumer(config.inbound, route_map, amqp_url))
	}

	fn fetch_config(&self) -> Result<QueueConfig, AnyError> {
		let url = config_url(&self.config_host, &self.name);
		let response = reqwest::blocking::get(url)?;
		let response = response.error_for_status()?;
		let config = response.json::<QueueConfig>()?;
		Ok(config)
	}

	async fn run_consumer(
		&self,
		inbound_queue: String,
		route_map: HashMap<String, Vec<String>>,
		amqp_url: String,
	) -> Result<(), AnyError> {
		let connection = Connection::connect(&amqp_url, ConnectionProperties::default()).await?;
		let channel = connection.create_channel().await?;

		declare_queues(&channel, &inbound_queue, &route_map).await?;

		let mut consumer = channel
			.basic_consume(
				&inbound_queue,
				&format!("{}-consumer", self.name),
				BasicConsumeOptions::default(),
				FieldTable::default(),
			)
			.await?;

		info!("Microservice '{}' started, consuming from queue '{}'", self.name, inbound_queue);
		while let Some(delivery_result) = consumer.next().await {
			let delivery = match delivery_result {
				Ok(delivery) => delivery,
				Err(err) => {
					error!("Error receiving message: {}", err);
					return Err(Box::new(err));
				}
			};

			let raw = std::str::from_utf8(&delivery.data)?;
			let request_id: u64 = match raw.trim().parse() {
				Ok(value) => value,
				Err(_) => {
					delivery.nack(BasicNackOptions::default()).await?;
					continue;
				}
			};

			let outputs = (self.process)(request_id);
			publish_outputs(&channel, outputs, &route_map).await?;
			delivery.ack(BasicAckOptions::default()).await?;
		}

		Ok(())
	}
}

#[derive(Debug, Deserialize)]
struct RabbitMqConfig {
	port: Vec<u16>,
	host: Vec<String>,
	username: Vec<String>,
	pass: Vec<String>,
}

fn fetch_rabbitmq_url_from_sys_map() -> Result<String, AnyError> {
	let response = reqwest::blocking::get("https://sys-map.slingshot.cv/rabbitmq")?;
	let response = response.error_for_status()?;
	let config = response.json::<RabbitMqConfig>()?;

	let port = single_value(&config.port, "port")?;
	let host = single_value(&config.host, "host")?;
	let username = single_value(&config.username, "username")?;
	let pass_key = single_value(&config.pass, "pass")?;
	let pass = resolve_password_from_pass(&pass_key)?;

	info!("Fetched RabbitMQ config from sys-map: host={}, port={}, username={}",
		host, port, username);

	Ok(format!("amqp://{}:{}@{}:{}/%2f", username, pass, host, port))
}

fn resolve_password_from_pass(pass_key: &str) -> Result<String, AnyError> {
	let output = Command::new("pass").arg("show").arg(pass_key).output()?;

	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
		let message = if stderr.is_empty() {
			format!("failed to resolve GNU pass entry '{}'", pass_key)
		} else {
			format!("failed to resolve GNU pass entry '{}': {}", pass_key, stderr)
		};
		return Err(message.into());
	}

	let stdout = String::from_utf8(output.stdout)?;
	let password = stdout.lines().next().unwrap_or("").trim().to_string();

	if password.is_empty() {
		return Err(format!("GNU pass entry '{}' returned an empty secret", pass_key).into());
	}

	Ok(password)
}

fn single_value<T: Clone>(values: &[T], field_name: &str) -> Result<T, AnyError> {
	if values.len() != 1 {
		return Err(format!(
			"sys-map.rabbitmq field '{}' must contain exactly one value, got {}",
			field_name,
			values.len()
		)
		.into());
	}

	Ok(values[0].clone())
}

fn config_url(host: &str, microservice_name: &str) -> String {
	if host.starts_with("http://") || host.starts_with("https://") {
		format!("{}/{}", host.trim_end_matches('/'), microservice_name)
	} else {
		format!("https://{}/{}", host.trim_end_matches('/'), microservice_name)
	}
}

fn build_route_map(outbound: &[OutboundCase]) -> HashMap<String, Vec<String>> {
	let mut map = HashMap::new();
	for entry in outbound {
		map.insert(case_key(&entry.case_value), entry.queues.clone());
	}
	map
}

fn case_key(case_value: &Value) -> String {
	case_value.to_string()
}

async fn declare_queues(
	channel: &Channel,
	inbound_queue: &str,
	route_map: &HashMap<String, Vec<String>>,
) -> Result<(), AnyError> {
	channel
		.queue_declare(
			inbound_queue,
			QueueDeclareOptions::default(),
			FieldTable::default(),
		)
		.await?;

	for queues in route_map.values() {
		for queue in queues {
			channel
				.queue_declare(
					queue,
					QueueDeclareOptions::default(),
					FieldTable::default(),
				)
				.await?;
		}
	}

	Ok(())
}

async fn publish_outputs(
	channel: &Channel,
	outputs: Vec<(u64, Value)>,
	route_map: &HashMap<String, Vec<String>>,
) -> Result<(), AnyError> {
	for (result_id, case_var) in outputs {
		if let Some(outbound_queues) = route_map.get(&case_key(&case_var)) {
			for queue in outbound_queues {
				let payload = result_id.to_string();
				let confirm = channel
					.basic_publish(
						"",
						queue,
						BasicPublishOptions::default(),
						payload.as_bytes(),
						BasicProperties::default(),
					)
					.await?;
				confirm.await?;
			}
		}
	}

	Ok(())
}
