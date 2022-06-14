use anyhow::Result;
use coding_test::processor::Processor;
use coding_test::reader::read_transactions_from_file;
use std::env;
use std::path::Path;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 || args[1] == "--help" {
        println!("Usage: <csv file path>");
        return Ok(());
    }
    let path = Path::new(&args[1]);
    let stream = read_transactions_from_file(path).await?;
    let mut processor = Processor::default();
    let clients = processor.process_transactions(stream).await?;
    let mut writer = csv::Writer::from_writer(std::io::stdout());
    for client in clients {
        writer.serialize(client)?;
    }
    writer.flush()?;

    Ok(())
}
