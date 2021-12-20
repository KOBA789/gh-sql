use std::sync::Mutex;

use anyhow::Result;
use async_trait::async_trait;
use gluesql::{
    ast::{ColumnDef, ColumnOption, ColumnOptionDef, DataType},
    data::{Row, Schema, ValueError},
    prelude::Value,
    result::{Error as GlueSQLError, Result as GlueSQLResult},
    store::{GStore, GStoreMut, RowIter, Store, StoreMut},
};
use serde::{Deserialize, Serialize};

use crate::github::{self, GraphQLResponse};

fn deserialize_json_string<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::de::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    let s: String = serde::de::Deserialize::deserialize(deserializer)?;
    serde_json::from_str(&s).map_err(serde::de::Error::custom)
}

struct Field {
    id: String,
    name: String,
    kind: FieldKind,
}

enum FieldKind {
    Normal,
    Option(Vec<FieldOption>),
    Iteration {
        #[allow(dead_code)]
        duration: u32,
        #[allow(dead_code)]
        start_day: u32,
        iterations: Vec<FieldIteration>,
        completed_iterations: Vec<FieldIteration>,
    },
}

struct FieldOption {
    id: String,
    name: String,
}

struct FieldIteration {
    id: String,
    title: String,
    duration: u32,
    start_date: String,
}

pub struct ProjectNextStorage {
    github: github::Client,
    owner: String,
    project_number: u32,
    cache: Mutex<Option<Cache>>,
}

pub struct Cache {
    project_id: String,
    fields: Vec<Field>,
    items: Vec<(String, Row)>,
}

impl Cache {
    fn items_schema(&self) -> Schema {
        let reserved_column_defs = [
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Text,
                options: vec![],
            },
            ColumnDef {
                name: "Repository".to_string(),
                data_type: DataType::Text,
                options: vec![],
            },
            ColumnDef {
                name: "Issue".to_string(),
                data_type: DataType::Int,
                options: vec![],
            },
            ColumnDef {
                name: "Title".to_string(),
                data_type: DataType::Text,
                options: vec![],
            },
            ColumnDef {
                name: "Assignees".to_string(),
                data_type: DataType::List,
                options: vec![],
            },
            ColumnDef {
                name: "Labels".to_string(),
                data_type: DataType::List,
                options: vec![],
            },
        ];
        let field_column_defs = self.fields.iter().map(|field| ColumnDef {
            name: field.name.to_string(),
            data_type: DataType::Text,
            options: vec![ColumnOptionDef {
                option: ColumnOption::Null,
                name: None,
            }],
        });
        let column_defs = reserved_column_defs
            .into_iter()
            .chain(field_column_defs)
            .collect();
        Schema {
            table_name: "items".to_string(),
            column_defs,
            indexes: vec![],
        }
    }

    fn scan_iterations(&self) -> RowIter<String> {
        #[allow(clippy::needless_collect)]
        let rows: Vec<_> = self
            .fields
            .iter()
            .filter_map(|field| {
                if let Field {
                    id: field_id,
                    kind:
                        FieldKind::Iteration {
                            iterations,
                            completed_iterations,
                            ..
                        },
                    ..
                } = field
                {
                    let iterations = iterations.iter().map(
                        |FieldIteration {
                             id,
                             title,
                             start_date,
                             duration,
                         }| {
                            let key = id.to_string();
                            let row = Row(vec![
                                Value::Str(field_id.to_string()),
                                Value::Str(id.to_string()),
                                Value::Str(title.to_string()),
                                Value::Str(start_date.to_string()),
                                Value::I64(*duration as i64),
                                Value::Bool(false),
                            ]);
                            (key, row)
                        },
                    );
                    let completed_iterations = completed_iterations.iter().map(
                        |FieldIteration {
                             id,
                             title,
                             start_date,
                             duration,
                         }| {
                            let key = id.to_string();
                            let row = Row(vec![
                                Value::Str(field_id.to_string()),
                                Value::Str(id.to_string()),
                                Value::Str(title.to_string()),
                                Value::Str(start_date.to_string()),
                                Value::I64(*duration as i64),
                                Value::Bool(true),
                            ]);
                            (key, row)
                        },
                    );
                    Some(iterations.chain(completed_iterations))
                } else {
                    None
                }
            })
            .flatten()
            .map(Ok)
            .collect();
        Box::new(rows.into_iter())
    }

    fn scan_options(&self) -> RowIter<String> {
        #[allow(clippy::needless_collect)]
        let rows: Vec<_> = self
            .fields
            .iter()
            .filter_map(|field| {
                if let Field {
                    id: field_id,
                    kind: FieldKind::Option(options),
                    ..
                } = field
                {
                    Some(options.iter().map(|FieldOption { id, name }| {
                        let key = id.to_string();
                        let row = Row(vec![
                            Value::Str(field_id.to_string()),
                            Value::Str(id.to_string()),
                            Value::Str(name.to_string()),
                        ]);
                        (key, row)
                    }))
                } else {
                    None
                }
            })
            .flatten()
            .map(Ok)
            .collect();
        Box::new(rows.into_iter())
    }
}

impl ProjectNextStorage {
    pub async fn new(github: github::Client, owner: String, project_number: u32) -> Result<Self> {
        Ok(Self {
            github,
            owner,
            project_number,
            cache: Mutex::new(None),
        })
    }

    async fn list_fields(&self) -> Result<(String, Vec<Field>)> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Variables {
            owner: String,
            project_number: u32,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            organization: Option<Organization>,
            user: Option<Organization>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Organization {
            project_next: ProjectNext,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ProjectNext {
            id: String,
            fields: FieldConnection,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct FieldConnection {
            nodes: Vec<FieldNode>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct FieldNode {
            id: String,
            name: String,
            #[serde(default)]
            #[serde(deserialize_with = "deserialize_json_string")]
            settings: Option<FieldSettings>,
        }
        #[derive(Deserialize)]
        struct FieldSettings {
            options: Option<Vec<FieldSettingsOption>>,
            configuration: Option<FieldSettingsConfiguration>,
        }
        #[derive(Deserialize)]
        struct FieldSettingsOption {
            id: String,
            name: String,
        }
        #[derive(Deserialize)]
        struct FieldSettingsConfiguration {
            duration: u32,
            start_day: u32,
            iterations: Vec<FieldSettingsConfigurationIteration>,
            completed_iterations: Vec<FieldSettingsConfigurationIteration>,
        }
        #[derive(Deserialize)]
        struct FieldSettingsConfigurationIteration {
            id: String,
            title: String,
            duration: u32,
            start_date: String,
        }
        impl From<FieldSettingsOption> for FieldOption {
            fn from(FieldSettingsOption { id, name }: FieldSettingsOption) -> Self {
                Self { id, name }
            }
        }
        impl From<FieldSettingsConfigurationIteration> for FieldIteration {
            fn from(
                FieldSettingsConfigurationIteration {
                    id,
                    title,
                    duration,
                    start_date,
                    ..
                }: FieldSettingsConfigurationIteration,
            ) -> Self {
                Self {
                    id,
                    title,
                    duration,
                    start_date,
                }
            }
        }
        let query = include_str!("list_fields.graphql");
        let variables = Variables {
            owner: self.owner.clone(),
            project_number: self.project_number,
        };
        let resp: github::GraphQLResponse<Response> =
            self.github.graphql(query, &variables).await?;
        let project_next = resp
            .data
            .organization
            .map(|org| org.project_next)
            .or_else(|| resp.data.user.map(|user| user.project_next));
        let project_next = if let Some(project_next) = project_next {
            project_next
        } else {
            return Err(anyhow::anyhow!(
                "No such user or organization: {}",
                self.owner
            ));
        };
        let project_id = project_next.id;
        let field_nodes = project_next.fields.nodes;
        let reserved_names = [
            "Title",
            "Labels",
            "Milestone",
            "Assignees",
            "Linked Pull Requests",
            "Reviewers",
            "Repository",
        ];
        let fields = field_nodes
            .into_iter()
            .filter(|field| reserved_names.iter().all(|&name| name != field.name))
            .map(|FieldNode { id, name, settings }| {
                let kind = if let Some(settings) = settings {
                    if let Some(options) = settings.options {
                        let options = options.into_iter().map(Into::into).collect();
                        FieldKind::Option(options)
                    } else if let Some(FieldSettingsConfiguration {
                        duration,
                        start_day,
                        iterations,
                        completed_iterations,
                    }) = settings.configuration
                    {
                        FieldKind::Iteration {
                            duration,
                            start_day,
                            iterations: iterations.into_iter().map(Into::into).collect(),
                            completed_iterations: completed_iterations
                                .into_iter()
                                .map(Into::into)
                                .collect(),
                        }
                    } else {
                        FieldKind::Normal
                    }
                } else {
                    FieldKind::Normal
                };
                Field { id, name, kind }
            })
            .collect();
        Ok((project_id, fields))
    }

    async fn scan_items(&self, project_id: String, fields: &[Field]) -> Result<Vec<(String, Row)>> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Variables {
            project_id: String,
            after: Option<String>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            node: ProjectNext,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ProjectNext {
            items: ProjectNextItemConnection,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ProjectNextItemConnection {
            page_info: PageInfo,
            nodes: Vec<ProjectNextItem>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ProjectNextItem {
            id: String,
            title: String,
            content: Option<ProjectNextItemContent>,
            field_values: ProjectNextItemFieldValueConnection,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ProjectNextItemContent {
            repository: Repository,
            number: u64,
            labels: LabelConnection,
            assignees: UserConnection,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct LabelConnection {
            nodes: Vec<Label>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Label {
            name: String,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct UserConnection {
            nodes: Vec<User>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct User {
            login: String,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Repository {
            owner: RepositoryOwner,
            name: String,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RepositoryOwner {
            login: String,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ProjectNextItemFieldValueConnection {
            nodes: Vec<ProjectNextItemFieldValue>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct PageInfo {
            has_next_page: bool,
            end_cursor: Option<String>,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ProjectNextItemFieldValue {
            project_field: ProjectNextField,
            value: String,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ProjectNextField {
            id: String,
        }
        let query = include_str!("list_items.graphql");
        let mut items = vec![];
        let mut after = None;
        while {
            let variables = Variables {
                project_id: project_id.clone(),
                after: after.clone(),
            };
            let resp: GraphQLResponse<Response> = self.github.graphql(query, &variables).await?;
            let ProjectNextItemConnection { page_info, nodes } = resp.data.node.items;
            items.extend(nodes);
            if let Some(end_cursor) = page_info.end_cursor {
                after = Some(end_cursor);
                page_info.has_next_page
            } else {
                false
            }
        } {}
        let rows: Vec<_> = items
            .into_iter()
            .map(|item| {
                let key = item.id;
                let (repo, issue, assignees, labels) = match item.content {
                    Some(content) => {
                        let repo = format!(
                            "{}/{}",
                            content.repository.owner.login, content.repository.name
                        );
                        let assignees = content
                            .assignees
                            .nodes
                            .into_iter()
                            .map(|u| Value::Str(u.login))
                            .collect();
                        let labels = content
                            .labels
                            .nodes
                            .into_iter()
                            .map(|l| Value::Str(l.name))
                            .collect();
                        (
                            Value::Str(repo),
                            Value::I64(content.number as i64),
                            Value::List(assignees),
                            Value::List(labels),
                        )
                    }
                    None => (Value::Null, Value::Null, Value::Null, Value::Null),
                };
                let reserved_columns = [
                    Value::Str(key.clone()),
                    repo,
                    issue,
                    Value::Str(item.title),
                    assignees,
                    labels,
                ];
                let field_columns = fields.iter().map(|field| {
                    let value = item
                        .field_values
                        .nodes
                        .iter()
                        .find(|value| value.project_field.id == field.id);
                    match value {
                        Some(value) => match &field.kind {
                            FieldKind::Normal => Value::Str(value.value.clone()),
                            FieldKind::Option(options) => {
                                if let Some(opt) = options.iter().find(|opt| opt.id == value.value)
                                {
                                    Value::Str(opt.name.clone())
                                } else {
                                    Value::Str("Unknown".to_string())
                                }
                            }
                            FieldKind::Iteration {
                                iterations,
                                completed_iterations,
                                ..
                            } => {
                                if let Some(iter) =
                                    iterations.iter().find(|iter| iter.id == value.value)
                                {
                                    Value::Str(iter.title.clone())
                                } else if let Some(iter) = completed_iterations
                                    .iter()
                                    .find(|iter| iter.id == value.value)
                                {
                                    Value::Str(iter.title.clone())
                                } else {
                                    Value::Str("Unknown".to_string())
                                }
                            }
                        },
                        None => Value::Null,
                    }
                });
                let row = Row(reserved_columns.into_iter().chain(field_columns).collect());
                (key, row)
            })
            .collect();
        Ok(rows)
    }

    fn iterations_schema() -> Schema {
        let column_defs = vec![
            ColumnDef {
                name: "field_id".to_string(),
                data_type: DataType::Text,
                options: vec![],
            },
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Text,
                options: vec![],
            },
            ColumnDef {
                name: "title".to_string(),
                data_type: DataType::Text,
                options: vec![],
            },
            ColumnDef {
                name: "duration".to_string(),
                data_type: DataType::Int,
                options: vec![],
            },
            ColumnDef {
                name: "start_date".to_string(),
                data_type: DataType::Text,
                options: vec![],
            },
            ColumnDef {
                name: "is_completed".to_string(),
                data_type: DataType::Boolean,
                options: vec![],
            },
        ];
        Schema {
            table_name: "iterations".to_string(),
            column_defs,
            indexes: vec![],
        }
    }

    fn options_schema() -> Schema {
        let column_defs = vec![
            ColumnDef {
                name: "field_id".to_string(),
                data_type: DataType::Text,
                options: vec![],
            },
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Text,
                options: vec![],
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
                options: vec![],
            },
        ];
        Schema {
            table_name: "options".to_string(),
            column_defs,
            indexes: vec![],
        }
    }

    async fn fetch_data(&self) -> Result<Cache> {
        let (project_id, fields) = self.list_fields().await?;
        let items = self.scan_items(project_id.clone(), &fields).await?;
        Ok(Cache {
            project_id,
            fields,
            items,
        })
    }

    async fn update_item_field(
        &self,
        project_id: String,
        item_id: String,
        field_id: String,
        value: String,
    ) -> Result<()> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Variables {
            project_id: String,
            item_id: String,
            field_id: String,
            value: String,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {}
        let query = include_str!("./update_item_field.graphql");
        let variables = Variables {
            project_id,
            item_id,
            field_id,
            value,
        };
        let resp: GraphQLResponse<Response> = self.github.graphql(query, &variables).await?;
        if !resp.errors.is_empty() {
            return Err(anyhow::anyhow!("Error: {:?}", resp.errors));
        }
        Ok(())
    }

    async fn delete_item_field(&self, project_id: String, item_id: String) -> Result<()> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Variables {
            project_id: String,
            item_id: String,
        }
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {}
        let query = include_str!("./delete_item.graphql");
        let variables = Variables {
            project_id,
            item_id,
        };
        let resp: GraphQLResponse<Response> = self.github.graphql(query, &variables).await?;
        if !resp.errors.is_empty() {
            return Err(anyhow::anyhow!("Error: {:?}", resp.errors));
        }
        Ok(())
    }
}

#[async_trait(?Send)]
impl Store<String> for ProjectNextStorage {
    async fn fetch_schema(&self, table_name: &str) -> GlueSQLResult<Option<Schema>> {
        let mut cache = self.cache.lock().unwrap();
        if cache.is_none() {
            *cache = Some(
                self.fetch_data()
                    .await
                    .map_err(|e| GlueSQLError::Storage(e.into()))?,
            );
        }
        let cache = cache.as_ref().unwrap();
        Ok(match table_name {
            "items" => Some(cache.items_schema()),
            "options" => Some(Self::options_schema()),
            "iterations" => Some(Self::iterations_schema()),
            _ => None,
        })
    }

    async fn scan_data(&self, table_name: &str) -> GlueSQLResult<RowIter<String>> {
        let mut cache = self.cache.lock().unwrap();
        if cache.is_none() {
            *cache = Some(
                self.fetch_data()
                    .await
                    .map_err(|e| GlueSQLError::Storage(e.into()))?,
            );
        }
        let cache = cache.as_ref().unwrap();
        match table_name {
            "items" => Ok(Box::new(cache.items.clone().into_iter().map(Ok))),
            "options" => Ok(cache.scan_options()),
            "iterations" => Ok(cache.scan_iterations()),
            _ => unreachable!(),
        }
    }
}

#[async_trait(?Send)]
impl StoreMut<String> for ProjectNextStorage {
    async fn insert_schema(self, _schema: &Schema) -> gluesql::result::MutResult<Self, ()> {
        todo!()
    }

    async fn delete_schema(self, _table_name: &str) -> gluesql::result::MutResult<Self, ()> {
        todo!()
    }

    async fn insert_data(
        self,
        _table_name: &str,
        _rows: Vec<Row>,
    ) -> gluesql::result::MutResult<Self, ()> {
        todo!()
    }

    async fn update_data(
        self,
        table_name: &str,
        rows: Vec<(String, Row)>,
    ) -> gluesql::result::MutResult<Self, ()> {
        if table_name != "items" {
            return Err((self, GlueSQLError::StorageMsg("readonly table".to_string())));
        }
        let mut cache_guard = self.cache.lock().unwrap();
        let cache = cache_guard.take().unwrap();
        drop(cache_guard);
        let schema = cache.items_schema();
        for (item_id, new_row) in rows {
            if let Some((_, org_row)) = cache.items.iter().find(|(org_id, _)| org_id == &item_id) {
                const RESERVED_COLS: usize = 6; // FIXME
                for (col_idx, (new_value, org_value)) in new_row.0[..RESERVED_COLS]
                    .iter()
                    .zip(org_row.0[..RESERVED_COLS].iter())
                    .enumerate()
                {
                    if new_value.is_null() && org_value.is_null() {
                        continue;
                    }
                    if new_value == org_value {
                        continue;
                    }
                    let col_name = &schema.column_defs[col_idx].name;
                    return Err((
                        self,
                        GlueSQLError::StorageMsg(format!("readonly column: {}", col_name)),
                    ));
                }
                for (field_idx, (new_value, org_value)) in new_row.0[RESERVED_COLS..]
                    .iter()
                    .zip(org_row.0[RESERVED_COLS..].iter())
                    .enumerate()
                {
                    if new_value.is_null() && org_value.is_null() {
                        continue;
                    }
                    if new_value == org_value {
                        continue;
                    }
                    let new_value_str = match new_value {
                        Value::Str(s) => Some(s.to_string()),
                        Value::Null => None,
                        _ => {
                            return Err((
                                self,
                                GlueSQLError::Value(ValueError::IncompatibleDataType {
                                    data_type: DataType::Text,
                                    value: new_value.clone(),
                                }),
                            ));
                        }
                    };
                    let field = &cache.fields[field_idx];
                    let new_value_gql = if let Some(new_value_str) = new_value_str {
                        match &field.kind {
                            FieldKind::Normal => new_value_str,
                            FieldKind::Option(options) => {
                                if let Some(opt) =
                                    options.iter().find(|opt| opt.name == new_value_str)
                                {
                                    opt.id.clone()
                                } else {
                                    return Err((
                                        self,
                                        GlueSQLError::Value(ValueError::ImpossibleCast),
                                    ));
                                }
                            }
                            FieldKind::Iteration {
                                iterations,
                                completed_iterations,
                                ..
                            } => {
                                if let Some(opt) =
                                    iterations.iter().find(|it| it.title == new_value_str)
                                {
                                    opt.id.clone()
                                } else if let Some(it) = completed_iterations
                                    .iter()
                                    .find(|opt| opt.title == new_value_str)
                                {
                                    it.id.clone()
                                } else {
                                    return Err((
                                        self,
                                        GlueSQLError::Value(ValueError::ImpossibleCast),
                                    ));
                                }
                            }
                        }
                    } else {
                        String::new()
                    };
                    if let Err(e) = self
                        .update_item_field(
                            cache.project_id.clone(),
                            item_id.clone(),
                            field.id.clone(),
                            new_value_gql,
                        )
                        .await
                    {
                        return Err((self, GlueSQLError::Storage(e.into())));
                    }
                }
            }
        }
        Ok((self, ()))
    }

    async fn delete_data(
        self,
        table_name: &str,
        keys: Vec<String>,
    ) -> gluesql::result::MutResult<Self, ()> {
        if table_name != "items" {
            return Err((self, GlueSQLError::StorageMsg("readonly table".to_string())));
        }
        let mut cache_guard = self.cache.lock().unwrap();
        let cache = cache_guard.take().unwrap();
        drop(cache_guard);
        for item_id in keys {
            if let Err(e) = self
                .delete_item_field(cache.project_id.clone(), item_id)
                .await
            {
                return Err((self, GlueSQLError::Storage(e.into())));
            }
        }
        Ok((self, ()))
    }
}

impl GStore<String> for ProjectNextStorage {}
impl GStoreMut<String> for ProjectNextStorage {}
