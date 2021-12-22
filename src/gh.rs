use anyhow::{anyhow, Context, Result};
use serde::{de::DeserializeOwned, Serialize, Deserialize};

use std::{
    io::Write,
    process::{Command, Stdio},
};

pub fn graphql<V, T>(query: &str, variables: &V) -> Result<T>
where
    V: Serialize,
    T: DeserializeOwned,
{
    #[derive(Debug, Serialize)]
    struct ReqBody<'a, V> {
        query: &'a str,
        variables: &'a V,
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
    let resp = serde_json::from_slice(&output.stdout).context("Failed to parse response")?;
    Ok(resp)
}

#[derive(Debug, Clone, Deserialize)]
pub struct GraphQLResponse<T, E = serde_json::Value> {
    pub data: T,
    #[serde(default = "Vec::new")]
    pub errors: Vec<E>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GraphQLError {
    #[serde(rename = "type")]
    pub typ: String,
    pub path: Vec<String>,
}
