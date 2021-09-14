use std::collections::HashMap;
use serde_json::{Value, json};
use reqwest::blocking::Client;
use std::error::Error;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opts {
    #[structopt(short, long, default_value = "http://localhost:9200")]
    from_cluster: String,
    #[structopt(short, long, default_value = "http://localhost:9400")]
    to_cluster: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let opts = Opts::from_args();
    let client = reqwest::blocking::Client::new();
    let mut indices: Vec<String> = Vec::new();
    let mut aliases: HashMap<String, String> = HashMap::new();
    get_indices_aliases(&opts, &client, &mut indices, &mut aliases)?;
    add_settings_mappings(&opts, &client, &indices)?;
    reindex(&opts, &client, &indices)?;
    add_aliases(&opts, &client, &aliases)?;
    Ok(())
}

fn get_indices_aliases(opts: &Opts, client: &Client, indices: &mut Vec<String>, aliases: &mut HashMap<String, String>) -> Result<(), Box<dyn Error>> {
    let from_cluster_aliases_url = format!("{}/_alias", opts.from_cluster);
    let resp = client.get(from_cluster_aliases_url).send()?
        .json::<HashMap<String, HashMap<String, HashMap<String, HashMap<String, String>>>>>()?;
    resp.keys().for_each(|k| {
        indices.push(k.to_string());
    });
    resp.iter().for_each(|(k, val)| {
        if let Some(alias) = val.get("aliases").unwrap().keys().next() {
            aliases.insert(k.to_string(), alias.to_string());
        }
    });
    Ok(())
}

fn add_aliases(opts: &Opts, client: &Client, aliases: &HashMap<String, String>) -> Result<(), Box<dyn Error>> {
    for (index, alias) in aliases {
        let url = format!("{}/{}/_alias/{}", opts.to_cluster, index, alias);
        let resp = client.put(url.as_str()).send()?;
        println!("add_alias for index: {} status: {:?}", index, resp.status());
    }
    Ok(())
}

fn reindex(opts: &Opts, client: &Client, indices: &Vec<String>) -> Result<(), Box<dyn Error>>{
    let index_url = format!("{}/_reindex", opts.to_cluster);
    for index in indices {
        let json = json!({
          "source": {
            "remote": {
              // "host": "http://172.19.0.1:9200"
              "host": opts.from_cluster
            },
            "index": index
          },
          "dest": {
            "index": index
          }
        });
        let resp = client.post(index_url.as_str())
            .json::<serde_json::Value>(&json)
            .send()?;
        println!("re-index index: {} status: {:?}", index, resp.status());
    }
    Ok(())
}

fn add_settings_mappings(opts: &Opts, client: &Client, indices: &Vec<String>) -> Result<(), Box<dyn Error>>{
    for index in indices {
        let index_url = &format!("{}/{}", opts.from_cluster, index);
        let resp = client.get(index_url).send()?.json::<serde_json::Value>()?;
        let mappings = resp[index]["mappings"].clone();
        let settings = resp[index]["settings"].to_string();
        let mut settings_value: HashMap<String, HashMap<String, Value>> = serde_json::from_str(settings.as_str())?;
        let mut settings_value_index = settings_value.get("index").unwrap().clone();
        settings_value_index.remove("creation_date");
        settings_value_index.remove("provided_name");
        settings_value_index.remove("uuid");
        settings_value_index.remove("version");
        settings_value.insert("index".to_string(), settings_value_index);
        let new_settings = serde_json::to_value(settings_value)?;
        let mut m = serde_json::Map::new();
        m.insert("mappings".to_string(), mappings);
        m.insert("settings".to_string(), new_settings);
        let settings_mappings = serde_json::Value::Object(m);

        let index_url = &format!("{}/{}", opts.to_cluster, index);
        let resp = client.put(index_url.as_str())
            .json::<serde_json::Value>(&settings_mappings)
            .send()?;
        println!("add_settings_mapping for index: {} status: {:?}", index, resp.status());
    }
    Ok(())
}


