use std::collections::HashMap;

use clap::Parser;
use iceberg::{table::Table, Catalog};
use iceberg_catalog_glue::{
    GlueCatalog, GlueCatalogConfig, AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
};
use serde::Deserialize;
// try!(something that produces result, "error message to log")

macro_rules! try_ok {
    ($e:expr, $m:literal) => {{
        let res = match $e {
            Ok(res) => res,
            Err(e) => {
                eprint!("{}: {}", $m, e);
                return;
            }
        };
        res
    }};
}

#[derive(Parser)]
struct Args {
    #[arg(short)]
    config_path: String,
}

// TODO: use clap + config
#[derive(Deserialize)]
struct Config {
    access_key_id: String,
    secret_access_key: String,
    session_token: String,
    region: String,
    warehouse: String,
    namespace: String,
    table: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let config = try_ok!(
        std::fs::read_to_string(&args.config_path),
        "Failed to read config file from path"
    );

    let config = try_ok!(toml::from_str(&config), "Failed to parse config");

    let table = load_table(config).await;
    list_partitions(table).await;
}

async fn list_partitions(table: Table) {
    let metadata = table.metadata().to_owned();
    // println!("metadata: {metadata:#?}");
    // let current_snapshot = metadata
    //     .current_snapshot()
    //     .expect("can this even be empty?")
    //     .to_owned();

    // get partitions
    let partition_spec = metadata.default_partition_spec().to_owned();
    println!("partition: {:#?}", partition_spec.fields());
}

async fn load_table(config: Config) -> Table {
    let glue_config = GlueCatalogConfig::builder()
        .props(HashMap::from([
            (AWS_ACCESS_KEY_ID.to_owned(), config.access_key_id),
            (AWS_SECRET_ACCESS_KEY.to_owned(), config.secret_access_key),
            (AWS_SESSION_TOKEN.to_owned(), config.session_token),
            (AWS_REGION_NAME.to_string(), config.region),
        ]))
        // this is the path to your metadata and data files
        // this is required to build FileIO based on the scheme of this path
        // FileIO helps you read and write to your warehouse
        .warehouse(config.warehouse)
        .build();

    // TODO: handle errors here instead of unwraps
    let table_ident = iceberg::TableIdent::from_strs(vec![config.namespace, config.table])
        .expect("Failed to create table identifier");

    let glue_catalog = GlueCatalog::new(glue_config).await.unwrap();
    let table = glue_catalog
        .load_table(&table_ident)
        .await
        .expect("failed to load table");

    table
}
