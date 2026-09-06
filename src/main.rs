mod configuration;
mod input;

use anyhow::bail;
use clap::Parser;
use configuration::Configuration;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
struct CliArgs {
    #[arg(short, long, default_value = ".")]
    workdir: PathBuf,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = CliArgs::parse();

    let config_path = args.workdir.join("grpcrash.yaml");
    let Ok(config_yaml) = std::fs::read_to_string(&config_path) else {
        bail!("Could not find configuration file at {:?}", config_path)
    };

    let config: Configuration = serde_yaml::from_str(&config_yaml)?;

    println!("{:?}", config);

    Ok(())
}
