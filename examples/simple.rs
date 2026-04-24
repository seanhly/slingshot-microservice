use slingshot_microservice::Microservice;

fn process(request: u64) -> Vec<(u64, String)> {
    vec![(request, "case_a".to_string())]
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let microservice = Microservice::new("simple-microservice", "sys-map.slingshot.cv", process);

    microservice.start()?;
    Ok(())
}
