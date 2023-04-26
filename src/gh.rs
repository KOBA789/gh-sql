use anyhow::{anyhow, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use std::{
    io::Write,
    process::{Command, Stdio},
};

pub fn graphql<V, T>(query: &str, variables: &V) -> Result<GraphQLResponse<T, GraphQLErrors>>
where
    V: Serialize,
    T: DeserializeOwned,
{
    #[derive(Debug, Serialize)]
    struct ReqBody<'a, V> {
        query: &'a str,
        variables: &'a V,
    }
    #[derive(Debug, Clone, Deserialize)]
    struct RespBody<T> {
        data: T,
    }

    let req_body = ReqBody { query, variables };
    let req_body_bytes =
        serde_json::to_vec(&req_body).context("Failed to serialize request body")?;

    let mut gh = Command::new("gh")
        .args(["api", "graphql", "--input", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("Failed to execute `gh` command")?;
    let stdin = gh.stdin.as_mut().expect("stdin is piped");
    stdin
        .write_all(&req_body_bytes)
        .context("Failed to write request body to stdin of `gh`")?;
    let output = gh
        .wait_with_output()
        .context("Failed to read response from `gh`")?;
    if !output.status.success() {
        let stderr = std::str::from_utf8(&output.stderr).unwrap_or_default();
        let code = output.status.code().expect("process has been exited");
        anyhow!("`gh` exited with status code: {}\n{}", code, stderr);
    }
    let err_resp: serde_json::Result<GraphQLErrors> = serde_json::from_slice(&output.stdout);
    let data_resp: RespBody<T> = match serde_json::from_slice(&output.stdout) {
        Ok(d) => d,
        Err(de) => {
            let de = anyhow::Error::new(de).context("Failed to parse response");
            return Err(match err_resp {
                Ok(e) => {
                    let error_msgs = e.error_msgs();

                    de.context(error_msgs)
                }
                Err(ee) => de.context(ee).context("Failed to parse error response"),
            });
        }
    };

    Ok(GraphQLResponse {
        data: data_resp.data,
        errors: err_resp.unwrap_or_default(),
    })
}

#[derive(Debug, Clone)]
pub struct GraphQLResponse<T, E = GraphQLErrors> {
    pub data: T,
    pub errors: E,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GraphQLErrors {
    #[serde(default)]
    pub errors: Vec<GraphQLError>,
}

impl GraphQLErrors {
    pub fn error_msgs(&self) -> String {
        self.errors
            .iter()
            .map(|e| e.message.as_str())
            .collect::<Vec<_>>()
            .join(" / ")
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GraphQLError {
    #[serde(default = "Vec::new")]
    pub path: Vec<ObjectPath>,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ObjectPath {
    Number(usize),
    String(String),
}
