use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::{Cursor, ErrorKind, Read};
#[cfg(feature = "python")]
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
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
use tokio::io::AsyncReadExt;
use tracing_subscriber::EnvFilter;

#[cfg(feature = "python")]
use pyo3::exceptions::{PyRuntimeError, PyTypeError};
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::{PyAny, PyBool, PyBytes, PyInt, PyModule, PyString};

pub type AnyError = Box<dyn Error + Send + Sync + 'static>;

pub type ReadFile = Box<dyn Read + Send + 'static>;
pub type ReadFileFn = dyn Fn(&str, u64) -> Result<ReadFile, AnyError> + Send + Sync + 'static;
pub type WriteFileFn = dyn Fn(&str, u64) -> Result<File, AnyError> + Send + Sync + 'static;

type ProcessFn = dyn Fn(u64, Arc<ReadFileFn>, Arc<WriteFileFn>) -> Result<Vec<(u64, CaseKey)>, AnyError>
	+ Send
	+ Sync
	+ 'static;

static REQUEST_FILE_CONTEXT_COUNTER: AtomicU64 = AtomicU64::new(1);

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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum CaseKey {
	Bool(bool),
	Int(i128),
	String(String),
}

/// A simple queue-driven microservice runtime.
///
/// The microservice:
/// 1) Retrieves queue metadata from a configuration service,
/// 2) Consumes u64 IDs from an inbound queue,
/// 3) Runs the user-provided processing function with S3-backed file helpers,
/// 4) Closes/finalizes staged files and uploads writes,
/// 5) Routes each output ID to outbound queue(s) based on case variables.
pub struct Microservice {
	name: String,
	config_host: String,
	process: Arc<ProcessFn>,
}

impl Microservice {
	fn new_case_key(
		name: impl Into<String>,
		config_host: impl Into<String>,
		process: Arc<ProcessFn>,
	) -> Self {
		init_tracing();
		Self {
			name: name.into(),
			config_host: config_host.into(),
			process,
		}
	}

	/// Create a new microservice runtime.
	///
	/// `process` accepts an inbound request ID, a `read_file` function, and a
	/// `write_file` function, and then returns a list of
	/// `(result_id, case_variable)` tuples. Case variables can be any
	/// serializable primitive, such as `String`, `bool`, or integers.
	pub fn new<F, C>(name: impl Into<String>, config_host: impl Into<String>, process: F) -> Self
	where
		F: Fn(u64, &ReadFileFn, &WriteFileFn) -> Result<Vec<(u64, C)>, AnyError> + Send + Sync + 'static,
		C: Serialize + 'static,
	{
		init_tracing();
		let process_wrapper = move |
			request: u64,
			read_file: Arc<ReadFileFn>,
			write_file: Arc<WriteFileFn>,
		| -> Result<Vec<(u64, CaseKey)>, AnyError> {
			let outputs = process(request, read_file.as_ref(), write_file.as_ref())?;
			let mut mapped = Vec::with_capacity(outputs.len());
			for (id, case) in outputs {
				let value = serde_json::to_value(case)
					.map_err(|e| format!("case variable must be serializable to JSON: {}", e))?;
				mapped.push((id, case_key_from_value(&value)?));
			}

			Ok(mapped)
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
		let route_map = build_route_map(&config.out)?;
		let amqp_url = fetch_rabbitmq_url_from_sys_map()?;

		let runtime = tokio::runtime::Builder::new_multi_thread()
			.enable_all()
			.build()?;
		let s3_client = runtime.block_on(fetch_s3_client_from_sys_map())?;

		runtime.block_on(self.run_consumer(config.inbound, route_map, amqp_url, s3_client))
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
		route_map: HashMap<CaseKey, Vec<String>>,
		amqp_url: String,
		s3_client: Arc<Client>,
	) -> Result<(), AnyError> {
		let bucket_name_cache = Arc::new(Mutex::new(HashMap::<String, String>::new()));
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

			info!("Received request ID {} from queue.", request_id);

			let file_context = Arc::new(Mutex::new(RequestFileContext::new(request_id)?));
			let read_context = Arc::clone(&file_context);
			let write_context = Arc::clone(&file_context);
			let s3_read_client = Arc::clone(&s3_client);
			let read_bucket_cache = Arc::clone(&bucket_name_cache);
			let write_bucket_cache = Arc::clone(&bucket_name_cache);
			let config_host = self.config_host.clone();
			let microservice_name = self.name.clone();
			let read_config_host = config_host.clone();
			let read_microservice_name = microservice_name.clone();

			let read_file: Arc<ReadFileFn> = Arc::new(move |key: &str, id: u64| -> Result<ReadFile, AnyError> {
				let bucket = resolve_bucket_name(
					&read_config_host,
					&read_microservice_name,
					&read_bucket_cache,
					key,
				)?;
				info!("Resolved bucket name for key '{}': {}", key, bucket);
				let mut guard = read_context
					.lock()
					.map_err(|e| format!("file context lock poisoned for read_file: {}", e))?;
				guard.read_file(s3_read_client.as_ref(), &bucket, id)
			});

			let write_file: Arc<WriteFileFn> = Arc::new(move |key: &str, id: u64| -> Result<File, AnyError> {
				let bucket = resolve_bucket_name(
					&config_host,
					&microservice_name,
					&write_bucket_cache,
					key,
				)?;
				let mut guard = write_context
					.lock()
					.map_err(|e| format!("file context lock poisoned for write_file: {}", e))?;
				guard.write_file(&bucket, id)
			});

			let outputs = (self.process)(request_id, Arc::clone(&read_file), Arc::clone(&write_file))?;
			{
				let mut guard = file_context
					.lock()
					.map_err(|e| format!("file context lock poisoned for finalize: {}", e))?;
				guard.finalize(s3_client.as_ref())?
			}
			publish_outputs(&channel, outputs, &route_map).await?;
			delivery.ack(BasicAckOptions::default()).await?;
		}

		Ok(())
	}
}

#[derive(Debug)]
struct PendingUpload {
	bucket: String,
	object_key: String,
	local_path: PathBuf,
}

#[derive(Debug)]
struct RequestFileContext {
	root_dir: PathBuf,
	pending_uploads: Vec<PendingUpload>,
}

impl RequestFileContext {
	fn new(request_id: u64) -> Result<Self, AnyError> {
		let unique = REQUEST_FILE_CONTEXT_COUNTER.fetch_add(1, Ordering::Relaxed);
		let root_dir = std::env::temp_dir().join(format!(
			"slingshot-microservice-{}-{}-{}",
			std::process::id(),
			request_id,
			unique
		));

		fs::create_dir_all(root_dir.join("write"))?;

		Ok(Self {
			root_dir,
			pending_uploads: Vec::new(),
		})
	}

	fn read_file(&mut self, s3_client: &Client, bucket: &str, id: u64) -> Result<ReadFile, AnyError> {
		let bucket_name = bucket.to_string();
		let object_key = id.to_string();
		let client = s3_client.clone();

		tokio::task::block_in_place(|| {
			tokio::runtime::Handle::current().block_on(async move {
				info!("Reading file from S3 - bucket: '{}', key: '{}'", bucket_name, object_key);
				let response = client
					.get_object()
					.bucket(&bucket_name)
					.key(&object_key)
					.send()
					.await
					.map_err(|e| {
						format!(
							"failed to get S3 object bucket='{}' key='{}': {:?}",
							bucket_name, object_key, e
						)
					})?;
				let mut stream = response.body.into_async_read();
				let mut bytes = Vec::new();
				stream.read_to_end(&mut bytes).await?;

				Ok::<ReadFile, AnyError>(Box::new(Cursor::new(bytes)))
			})
		})
	}

	fn write_file(&mut self, key: &str, id: u64) -> Result<File, AnyError> {
		let object_key = id.to_string();
		let local_path = self
			.root_dir
			.join("write")
			.join(format!("{}.bin", self.pending_uploads.len()));

		self.pending_uploads.push(PendingUpload {
			bucket: key.to_string(),
			object_key,
			local_path: local_path.clone(),
		});

		Ok(File::create(local_path)?)
	}

	fn finalize(&mut self, s3_client: &Client) -> Result<(), AnyError> {
		for upload in &self.pending_uploads {
			upload_to_s3(s3_client, &upload.bucket, &upload.object_key, &upload.local_path)?;
			remove_if_exists(&upload.local_path)?;
		}

		self.pending_uploads.clear();
		remove_dir_if_exists(&self.root_dir)?;

		Ok(())
	}
}

impl Drop for RequestFileContext {
	fn drop(&mut self) {
		for upload in &self.pending_uploads {
			let _ = remove_if_exists(&upload.local_path);
		}
		let _ = remove_dir_if_exists(&self.root_dir);
	}
}

fn upload_to_s3(
	s3_client: &Client,
	bucket: &str,
	object_key: &str,
	local_path: &Path,
) -> Result<(), AnyError> {
	let client = s3_client.clone();
	let bucket_name = bucket.to_string();
	let key = object_key.to_string();
	let path = local_path.to_path_buf();

	tokio::task::block_in_place(|| {
		tokio::runtime::Handle::current().block_on(async move {
			let body = ByteStream::from_path(&path).await?;
			client
				.put_object()
				.bucket(&bucket_name)
				.key(&key)
				.body(body)
				.send()
				.await?;

			Ok::<(), AnyError>(())
		})
	})
}

fn remove_if_exists(path: &Path) -> Result<(), AnyError> {
	match fs::remove_file(path) {
		Ok(()) => Ok(()),
		Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
		Err(err) => Err(Box::new(err)),
	}
}

fn remove_dir_if_exists(path: &Path) -> Result<(), AnyError> {
	match fs::remove_dir_all(path) {
		Ok(()) => Ok(()),
		Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
		Err(err) => Err(Box::new(err)),
	}
}

#[derive(Debug, Deserialize)]
struct RabbitMqConfig {
	port: Vec<u16>,
	host: Vec<String>,
	username: Vec<String>,
	pass: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ObjectStorageConfig {
	host: Vec<String>,
	#[serde(rename = "pass:access-key")]
	pass_access_key: Vec<String>,
	#[serde(rename = "pass:secret-key")]
	pass_secret_key: Vec<String>,
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

async fn fetch_s3_client_from_sys_map() -> Result<Arc<Client>, AnyError> {
	let response = reqwest::get("https://sys-map.slingshot.cv/object-storage").await?;
	let response = response.error_for_status()?;
	let config = response.json::<ObjectStorageConfig>().await?;

	let host = single_value(&config.host, "host")?;
	let access_key_ref = single_value(&config.pass_access_key, "pass:access-key")?;
	let secret_key_ref = single_value(&config.pass_secret_key, "pass:secret-key")?;
	let access_key = resolve_password_from_pass(&access_key_ref)?;
	let secret_key = resolve_password_from_pass(&secret_key_ref)?;

	info!("Fetched object storage config from sys-map: host={}", host);

	let shared_config = aws_config::defaults(BehaviorVersion::latest())
		.region(Region::new("garage"))
		.credentials_provider(Credentials::new(
			access_key,
			secret_key,
			None,
			None,
			"pass",
		))
		.load()
		.await;

	let s3_config = aws_sdk_s3::config::Builder::from(&shared_config)
		.endpoint_url(format!("https://{}", host))
		.force_path_style(true)
		.build();

	Ok(Arc::new(Client::from_conf(s3_config)))
}

fn resolve_bucket_name(
	config_host: &str,
	microservice_name: &str,
	cache: &Mutex<HashMap<String, String>>,
	key: &str,
) -> Result<String, AnyError> {
	{
		let guard = cache
			.lock()
			.map_err(|e| format!("bucket-name cache lock poisoned: {}", e))?;
		if let Some(bucket_name) = guard.get(key) {
			return Ok(bucket_name.clone());
		}
	}

	let url = bucket_mapping_url(config_host, microservice_name, key);
	let bucket_name = tokio::task::block_in_place(|| {
		tokio::runtime::Handle::current().block_on(async {
			let response = reqwest::get(&url).await?;
			let response = response.error_for_status()?;
			Ok::<String, AnyError>(response.text().await?.trim().to_string())
		})
	})?;

	if bucket_name.is_empty() {
		return Err(format!("bucket mapping '{}' returned an empty bucket name", url).into());
	}

	let mut guard = cache
		.lock()
		.map_err(|e| format!("bucket-name cache lock poisoned: {}", e))?;
	guard.insert(key.to_string(), bucket_name.clone());

	Ok(bucket_name)
}

fn resolve_password_from_pass(pass_key: &str) -> Result<String, AnyError> {
	let output = Command::new("pass").arg("show").arg(pass_key).output()?;

	info!("Resolving GNU pass entry '{}'", pass_key);

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
			"sys-map field '{}' must contain exactly one value, got {}",
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

fn bucket_mapping_url(host: &str, microservice_name: &str, key: &str) -> String {
	format!("{}/{}", config_url(host, microservice_name), key.trim_matches('/'))
}

fn build_route_map(outbound: &[OutboundCase]) -> Result<HashMap<CaseKey, Vec<String>>, AnyError> {
	let mut map = HashMap::new();
	for entry in outbound {
		let case_key: CaseKey = case_key_from_value(&entry.case_value)?;
		let case_type = match &case_key {
			CaseKey::Bool(_) => "bool",
			CaseKey::Int(_) => "int",
			CaseKey::String(_) => "string",
		};
		let case_key_str = match &case_key {
			CaseKey::Bool(value) => value.to_string(),
			CaseKey::Int(value) => value.to_string(),
			CaseKey::String(value) => value.clone(),
		};
		info!("Case variable with type:{} and value:{} maps to queues: {:?}", case_type, case_key_str, entry.queues);
		map.insert(case_key, entry.queues.clone());
	}
	Ok(map)
}

fn case_key_from_value(case_value: &Value) -> Result<CaseKey, AnyError> {
	match case_value {
		Value::Bool(value) => Ok(CaseKey::Bool(*value)),
		Value::String(value) => Ok(CaseKey::String(value.clone())),
		Value::Number(value) => {
			if let Some(v) = value.as_i64() {
				Ok(CaseKey::Int(v as i128))
			} else if let Some(v) = value.as_u64() {
				Ok(CaseKey::Int(v as i128))
			} else {
				Err(format!("case variable '{}' must be an integer number", value).into())
			}
		}
		_ => Err("case variable must be one of: bool, int, string".into()),
	}
}

async fn declare_queues(
	channel: &Channel,
	inbound_queue: &str,
	route_map: &HashMap<CaseKey, Vec<String>>,
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
	outputs: Vec<(u64, CaseKey)>,
	route_map: &HashMap<CaseKey, Vec<String>>,
) -> Result<(), AnyError> {
	for (result_id, case_var) in outputs {
		info!("Shuttle output result_id={}, case_var={:?}", result_id, case_var);
		if let Some(outbound_queues) = route_map.get(&case_var) {
			for queue in outbound_queues {
				info!("Publishing result ID {} to queue '{}' for case variable {:?}", result_id, queue, case_var);
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

#[cfg(feature = "python")]
fn any_error_to_py(err: AnyError) -> PyErr {
	PyRuntimeError::new_err(err.to_string())
}

#[cfg(feature = "python")]
fn case_key_from_py_value(value: &Bound<'_, PyAny>) -> PyResult<CaseKey> {
	if value.is_instance_of::<PyBool>() {
		return Ok(CaseKey::Bool(value.extract::<bool>()?));
	}

	if value.is_instance_of::<PyInt>() {
		return Ok(CaseKey::Int(value.extract::<i128>()?));
	}

	if value.is_instance_of::<PyString>() {
		return Ok(CaseKey::String(value.extract::<String>()?));
	}

	Err(PyTypeError::new_err(
		"case variable must be one of: bool, int, string",
	))
}

#[cfg(feature = "python")]
#[pyclass(name = "ReadFileFn")]
struct PyReadFileFn {
	inner: Arc<ReadFileFn>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyReadFileFn {
	fn __call__(&self, py: Python<'_>, key: &str, id: u64) -> PyResult<Py<PyAny>> {
		let mut reader = (self.inner)(key, id).map_err(any_error_to_py)?;
		let mut data = Vec::new();
		reader.read_to_end(&mut data).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

		let io = py.import("io")?;
		let bytes_io = io
			.getattr("BytesIO")?
			.call1((PyBytes::new(py, &data),))?;

		Ok(bytes_io.unbind())
	}
}

#[cfg(feature = "python")]
#[pyclass]
struct PyWriteHandle {
	file: Arc<Mutex<File>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyWriteHandle {
	fn write(&self, data: &[u8]) -> PyResult<usize> {
		let mut file = self
			.file
			.lock()
			.map_err(|e| PyRuntimeError::new_err(format!("write lock poisoned: {}", e)))?;
		file.write_all(data)
			.map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
		Ok(data.len())
	}

	fn flush(&self) -> PyResult<()> {
		let mut file = self
			.file
			.lock()
			.map_err(|e| PyRuntimeError::new_err(format!("flush lock poisoned: {}", e)))?;
		file.flush()
			.map_err(|e| PyRuntimeError::new_err(e.to_string()))
	}
}

#[cfg(feature = "python")]
#[pyclass(name = "WriteFileFn")]
struct PyWriteFileFn {
	inner: Arc<WriteFileFn>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyWriteFileFn {
	fn __call__(&self, py: Python<'_>, key: &str, id: u64) -> PyResult<Py<PyWriteHandle>> {
		let file = (self.inner)(key, id).map_err(any_error_to_py)?;
		Py::new(
			py,
			PyWriteHandle {
				file: Arc::new(Mutex::new(file)),
			},
		)
	}
}

#[cfg(feature = "python")]
fn run_python_process(
	process: &Py<PyAny>,
	request: u64,
	read_file: Arc<ReadFileFn>,
	write_file: Arc<WriteFileFn>,
) -> Result<Vec<(u64, CaseKey)>, AnyError> {
	Python::with_gil(|py| -> Result<Vec<(u64, CaseKey)>, AnyError> {
		let py_read = Py::new(py, PyReadFileFn { inner: read_file })
			.map_err(|e| format!("failed to build Python ReadFileFn wrapper: {}", e))?;
		let py_write = Py::new(py, PyWriteFileFn { inner: write_file })
			.map_err(|e| format!("failed to build Python WriteFileFn wrapper: {}", e))?;

		let returned = process
			.call1(py, (request, py_read, py_write))
			.map_err(|e| format!("Python process callback failed: {}", e))?;

		let iter = returned
			.bind(py)
			.try_iter()
			.map_err(|e| format!("process return value must be iterable: {}", e))?;

		let mut outputs = Vec::new();
		for item in iter {
			let item = item.map_err(|e| format!("failed to iterate process outputs: {}", e))?;
			let (id, case_obj): (u64, Py<PyAny>) = item
				.extract()
				.map_err(|e| format!("each output must be a tuple (int, case): {}", e))?;
			let case = case_key_from_py_value(case_obj.bind(py))
				.map_err(|e| format!("invalid case variable: {}", e))?;
			outputs.push((id, case));
		}

		Ok(outputs)
	})
}

#[cfg(feature = "python")]
#[pyclass(name = "Microservice")]
struct PyMicroservice {
	name: String,
	config_host: String,
	process: Py<PyAny>,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyMicroservice {
	#[new]
	fn new(name: String, config_host: String, process: Py<PyAny>) -> PyResult<Self> {
		Python::with_gil(|py| {
			if !process.bind(py).is_callable() {
				return Err(PyTypeError::new_err("process must be callable"));
			}

			Ok(Self {
				name,
				config_host,
				process,
			})
		})
	}

	fn start(&self) -> PyResult<()> {
		let process = Python::with_gil(|py| self.process.clone_ref(py));
		let microservice = Microservice::new_case_key(
			self.name.clone(),
			self.config_host.clone(),
			Arc::new(move |request, read_file, write_file| run_python_process(&process, request, read_file, write_file)),
		);

		microservice.start().map_err(any_error_to_py)
	}
}

#[cfg(feature = "python")]
#[pymodule]
fn _native(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
	module.add_class::<PyMicroservice>()?;

	Ok(())
}
