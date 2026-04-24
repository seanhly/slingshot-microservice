# `slingshot-microservice`: A Rust framework for standard microservice design

![](./docs/icons/256x256/slingshot-microservice.png)

`slingshot-microservice` is a Rust package that provides a simple, opinionated
framework for building microservices.  The framework makes the following
assumptions about a microservice:

1. A microservice listens to incoming requests on its own dedicated and
   singular queue (RabbitMQ).
2. Incoming requests are in the form of a 64-bit unsigned integer (`u64`).
2. Microservices process requests via a `process` function, which takes three
    arguments: the incoming request (`u64`), a `read_file` function, and a
    `write_file` function.
3. The `process` function returns a set of IDs (also `u64`) that are the result
   of processing the incoming request.  Each of these IDs is also associated
   with a "case variable" that is used for routing the result to the
   appropriate outbound queues.
4. Rather than hard-coding the inbound and outbound queues, the
   microservice communicates with a self-contained configuration service shared
   across all microservices.
   i.  This service provides inbound queue name, as well as any outbound queues
       and their corresponding case variables.
   ii. It is also responsible for providing the RabbitMQ connection details
	   (host, port, username, password), and the object-storage host plus GNU
	   `pass` references for the S3 access key and secret key.

The `slingshot-microservice` framework handles setting up the RabbitMQ
connection, listening to the inbound queue and routing results based on case variables.

## Adding The Framework To Your Project

Add `slingshot-microservice` to your `Cargo.toml` dependencies directly from Codeberg:

```toml
[dependencies]
slingshot-microservice = { git = "https://codeberg.org/seanhly/slingshot-microservice" }
```

Then fetch and build dependencies:

```bash
cargo build
```

## Example Usage

```rust
use slingshot_microservice::Microservice;
use slingshot_microservice::{AnyError, ReadFileFn, WriteFileFn};
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

fn main() {
    // Create a new microservice instance with the processing function
    let microservice = Microservice::new(
        "simple-microservice",
        "sys-map.example.com",
        process
    );

    // Start the microservice (this will block and listen for incoming requests)
    microservice.start();
}
```

## How it works:

The configuration service responds to requests of the form:
`https://{HOSTNAME}/{MICROSERVICE_NAME}`.  All configuration is done over HTTP
GET. The response contains a JSON object with two fields: an inbound queue name
and a mapping of case variables to outbound queue names. For example:

```json
{
    "in": "simple-microservice-inbound",
    "out": [
        {
            "case": "case_a",
            "queues": ["case_a_outbound_1", "case_a_outbound_2"]
        },
        {
            "case": "case_b",
            "queues": ["case_b_outbound"]
        }
    ]
}
```

The case variables can be any primitive type (e.g. string, integer, boolean).
E.g. a binary classification microservice might decide on which outbound queue
to send results to based on a case variable that is either `false` or `true`:

```json
{
    "in": "binary-classification-inbound",
    "out": [
        {
            "case": false,
            "queues": ["binary-classification-false-outbound"]
        },
        {
            "case": true,
            "queues": ["binary-classification-true-outbound"]
        }
    ]
}
```

The configuration service also provides the RabbitMQ connection details (host,
port, etc.):

Object storage credentials are fetched separately from
`https://sys-map.slingshot.cv/object-storage`. The access-key and secret-key
values returned there are GNU `pass` entry names, so the runtime resolves the
actual secrets with `pass show <key>` before constructing the S3 client.

When the microservice first starts up, it makes a request to the configuration
service to get the queue metadata.  Then it starts to listen to the inbound
queue.  Inbound requests are processed by the user-programmed `process`
function, which returns a set of tuples of the form `(result_id, case_variable)`.

Within each `process` pass:

1. `read_file(key, id)` treats `key` as a bucket reference such as `in`, not
    as the canonical bucket name. On first use, the runtime fetches
    `https://{HOSTNAME}/{MICROSERVICE_NAME}/{key}` to resolve the real bucket
    name, caches that mapping, and then returns a synchronous reader for object
    `id` in that bucket using the AWS SDK.
2. `write_file(key, id)` resolves `key` through the same cached lookup and
    returns an opened local file handle for writing, staging the output for
    `s3://{resolved_bucket}/{id}`.
3.  After `process` returns, opened files are closed.
4.  Then staged write files are uploaded to S3 with the AWS SDK, local staged
    files are deleted, and local temporary directories are removed.
5.  Only after file finalization is complete are output IDs published to
    outbound queues.

The output queue routing step looks like this:

Peudocode:
```
for each (result_id, case_variable) in process(request):
    for each outbound_queue in config.out[case_variable]:
        send result_id to outbound_queue
```
