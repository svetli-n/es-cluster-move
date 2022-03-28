mod move_cluster;

use crate::move_cluster::{move_cluster, Options};
use std::error::Error;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "Elasticsearch operations",
    about = "Common Elasticsearch operations."
)]
enum Opts {
    MoveCluster(Options),
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    match Opts::from_args() {
        Opts::MoveCluster(options) => move_cluster(options),
    }
}
