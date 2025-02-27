use std::{
    collections::{HashMap, HashSet},
    ops::DerefMut,
};

use chrono::Utc;
use clap::Parser;
use iceberg::{
    spec::{Literal, ManifestStatus},
    table::Table,
    Catalog,
};
use iceberg_catalog_glue::{
    GlueCatalog, GlueCatalogConfig, AWS_ACCESS_KEY_ID, AWS_REGION_NAME, AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
};
use serde::Deserialize;

// try_ok!(expr that returs Result, "error message to log")
// TODO: check $e is Result at compile time?
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
    get_schema(&table).await;
    list_partitions(&table).await;
}

async fn get_schema(table: &Table) {
    let schema = table.metadata().current_schema();
    println!("schema: {}", schema)
}

async fn list_partitions(table: &Table) {
    // struct Partitions {
    //     field: PartitionField,
    //     rows: usize,
    // }

    // impl Partitions {
    //     fn new(field: PartitionField) -> Self {
    //         Self { field, rows: 0 }
    //     }
    // }

    let mut partition_map: HashMap<i32, Vec<HashSet<Literal>>> = Default::default();
    // println!("metadata: {:#?}", table.metadata());

    let manifest_list = match table.metadata().current_snapshot() {
        None => {
            // Implies
            // - Brand new table without any commit
            // - All snapshots expired
            // - Corrupted metadata file
            return;
        }
        Some(snapshot) => {
            try_ok!(
                snapshot
                    .load_manifest_list(table.file_io(), table.metadata())
                    .await,
                "Failed to load manifest list"
            )
        }
    };

    for manifest in manifest_list.entries().iter().take(1) {
        let manifest = try_ok!(
            manifest.load_manifest(table.file_io()).await,
            "Failed to load manifest file"
        );

        let (entries, manifest_metadata) = manifest.into_parts();
        let current_partition_spec_id = manifest_metadata.partition_spec().spec_id();

        let m = partition_map
            .entry(current_partition_spec_id)
            .or_insert_with(|| {
                vec![
                    HashSet::default();
                    table
                        .metadata()
                        .partition_spec_by_id(current_partition_spec_id)
                        .unwrap()
                        .fields()
                        .len()
                ]
            });

        for entry in entries.iter() {
            if entry.status() == ManifestStatus::Deleted {
                // this file has been deleted,
                // do not account for its partitions
                continue;
            }

            for (field_id, partition_value) in
                entry.data_file().partition().fields().iter().enumerate()
            {
                m.deref_mut()
                    .get_mut(field_id)
                    .unwrap()
                    .insert(partition_value.to_owned());
            }
        }
    }

    println!("Active partitions");
    for (_i, (k, v)) in partition_map.iter().enumerate() {
        println!("partition spec id: {}", k);
        let partition_fields = table.metadata().partition_spec_by_id(*k).unwrap().fields();

        println!(
            "{:<20}|{:<20}|{:<20}|{:<20}",
            "id", "name", "transform", "distinct_count"
        );
        for (field_id, field_values) in v.iter().enumerate() {
            println!(
                "{:<20}|{:<20}|{:<20}|{:<20}",
                partition_fields[field_id].field_id,
                partition_fields[field_id].name,
                format!("{:?}", partition_fields[field_id].transform),
                field_values.len(),
            );
            // println!("\t{}: count {:?}", field_id, field_values.len())
            // if field_values.len() < 10 {
            //     println!("\t{}: {:?}", field_id, field_values)
            // } else {
            //     println!("\t{}: count {:?}", field_id, field_values.len())
            // }
        }
        println!();
    }
}

async fn load_table(config: Config) -> Table {
    let glue_config = GlueCatalogConfig::builder()
        .props(HashMap::from([
            (AWS_ACCESS_KEY_ID.to_owned(), config.access_key_id),
            (AWS_SECRET_ACCESS_KEY.to_owned(), config.secret_access_key),
            (AWS_SESSION_TOKEN.to_owned(), config.session_token),
            (AWS_REGION_NAME.to_string(), config.region),
        ]))
        // warehouse = parent path to metadata and data files
        //
        // FileIO is built based on the scheme of warehouse path
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

    let last_updated = table
        .metadata()
        .last_updated_timestamp()
        .expect("Last updated time must be valid");

    let updated_delta = Utc::now() - last_updated;

    println!(
        "table: {}.{}",
        table.identifier().namespace().join("."),
        table.identifier().name()
    );
    println!(
        "last updated: {} ({} minutes ago)",
        last_updated,
        updated_delta.num_minutes()
    );
    println!("version: {}", table.metadata().format_version());
    println!(
        "metadata path: {}",
        table
            .metadata_location()
            .expect("Metadata location must be set when loading a Glue table")
    );
    println!();

    table
}
