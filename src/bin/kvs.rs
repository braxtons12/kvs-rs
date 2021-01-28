#![deny(clippy::all, clippy::pedantic, clippy::cargo)]
#![allow(clippy::clippy::multiple_crate_versions)]

extern crate clap;
extern crate structopt;

use structopt::StructOpt;

use kvs::Commands;

/// kvs cli arguments representation
#[derive(Debug, StructOpt)]
#[structopt(name = "kvs", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"))]
pub struct Opts {
	#[structopt(subcommand)]
	pub commands: Commands,
}

fn main() -> anyhow::Result<()> {
	let args = Opts::from_args();
	let mut _logger = kvs::setup_loggers();

	match args.commands {
		Commands::Get { key } => {
			let mut kvs = kvs::KvStore::open(std::env::current_dir()?)?;
			if let Some(value) = kvs.get(key.clone())? {
				println!("{}", value);
			} else {
				println!("No value present for key: {}", key);
			}
		}
		Commands::Set { key, value } => {
			let mut kvs = kvs::KvStore::open(std::env::current_dir()?)?;
			kvs.set(key, value)?;
		}
		Commands::Rm { key } => {
			let mut kvs = kvs::KvStore::open(std::env::current_dir()?)?;
			kvs.remove(key)?;
		}
	}

	std::process::exit(0);
}
