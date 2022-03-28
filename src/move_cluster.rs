use std::collections::HashMap;
use serde_json::{Value, json};
use reqwest::blocking::{Client, Response};
use std::error::Error;
use structopt::StructOpt;
use reqwest::StatusCode;


#[derive(Debug, StructOpt, Clone)]
pub struct Options {
    #[structopt(short, long, default_value = "http://localhost:9200")]
    from_cluster: String,
    #[structopt(short, long, default_value = "http://localhost:9400")]
    to_cluster: String,
}

struct Reindexer {
    opts: Options,
    client: Client,
    indices: Vec<String>,
    aliases: HashMap<String, String>,
}

impl Reindexer {
    fn new(opts: Options) -> Self {
        Reindexer {
            opts,
            client: reqwest::blocking::Client::new(),
            indices: Default::default(),
            aliases: Default::default(),
        }
    }
}


pub fn move_cluster(opts: Options) -> Result<(), Box<dyn Error>> {

    let mut reindexer = Reindexer::new(opts);

    reindexer.set_indices_aliases()?;
    reindexer.add_settings_mappings()?;
    reindexer.reindex()?;
    reindexer.add_aliases()?;
    Ok(())
}

impl Reindexer {

    fn set_indices_aliases(&mut self) -> Result<(), Box<dyn Error>> {
        let from_cluster_aliases_url = format!("{}/_alias", self.opts.from_cluster);
        let resp = self.client.get(from_cluster_aliases_url).send()?
            .json::<HashMap<String, HashMap<String, HashMap<String, HashMap<String, String>>>>>()?;
        resp.iter().for_each(|(k, val)| {
            // skip internal indices
            if !k.starts_with(".") {
                self.indices.push(k.to_string());
                if let Some(alias) = val.get("aliases").unwrap().keys().next() {
                    self.aliases.insert(k.to_string(), alias.to_string());
                }
            }
        });
        Ok(())
    }

    fn add_aliases(&self) -> Result<(), Box<dyn Error>> {
        for (index, alias) in &self.aliases {
            let url = format!("{}/{}/_alias/{}", self.opts.to_cluster, index, alias);
            let resp = self.client.put(url.as_str()).send()?;
            log::info!("add_alias for index: {} status: {:?}", index, resp.status());
            log("add_aliases", index, resp)?;
        }
        Ok(())
    }

    fn reindex(&self) -> Result<(), Box<dyn Error>> {
        let index_url = format!("{}/_reindex", self.opts.to_cluster);
        for index in &self.indices {
            let json = json!({
          "source": {
            "remote": {
              "host": self.opts.from_cluster
            },
            "index": index
          },
          "dest": {
            "index": index
          }
        });
            let resp = self.client.post(index_url.as_str())
                .json::<serde_json::Value>(&json)
                .send()?;
            log("reindex", index, resp)?;
        }
        Ok(())
    }


    fn add_settings_mappings(&self) -> Result<(), Box<dyn Error>> {
        for index in &self.indices {
            let index_url = &format!("{}/{}", self.opts.from_cluster, index);
            let resp = self.client.get(index_url).send()?.json::<serde_json::Value>()?;
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

            let index_url = &format!("{}/{}", self.opts.to_cluster, index);
            let resp = self.client.put(index_url.as_str())
                .json::<serde_json::Value>(&settings_mappings)
                .send()?;
            log("add_settings_mappings", index, resp)?;
        }
        Ok(())
    }
}

fn log(method: &str, index: &String, resp: Response) -> Result<(), Box<dyn Error>>{
    log::info!("method: {} index: {} status: {:?}", method, index, resp.status());
    if resp.status() != StatusCode::OK {
        log::error!("method: {} index: {} error: {:?}", method, index, resp.text()?);
    }
    Ok(())
}
