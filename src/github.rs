use anyhow::Result;
use http::Method;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

pub struct Client {
    base_url: reqwest::Url,
    token: String,
    client: reqwest::Client,
}

impl Client {
    pub fn new(
        base_url: reqwest::Url,
        token: String,
        client: reqwest::Client,
    ) -> Self {
        Self {
            base_url,
            token,
            client,
        }
    }

    pub async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<reqwest::Body>,
    ) -> Result<reqwest::Response> {
        let url = reqwest::Url::options()
            .base_url(Some(&self.base_url))
            .parse(path)?;
        let mut req = self
            .client
            .request(method, url)
            .header(http::header::AUTHORIZATION, format!("token {}", self.token))
            .header(http::header::ACCEPT, "application/json")
            .header(http::header::USER_AGENT, USER_AGENT);
        if let Some(body) = body {
            req = req.body(body);
        }
        let res = req.send().await?;
        Ok(res)
    }

    pub async fn graphql<V, T>(&self, query: &str, variables: &V) -> Result<T>
    where
        V: Serialize,
        T: DeserializeOwned,
    {
        #[derive(Debug, Serialize)]
        struct ReqBody<'a, V> {
            query: &'a str,
            variables: &'a V,
        }
        let body = ReqBody { query, variables };
        let body = serde_json::to_vec(&body)?;
        let resp = self
            .request(http::Method::POST, "graphql", Some(body.into()))
            .await?;
        let resp = resp.error_for_status()?;
        Ok(resp.json().await?)
    }
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
