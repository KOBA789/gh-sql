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

use crate::gh::{self, GraphQLResponse};

struct Field {
    id: String,
    name: String,
    kind: FieldKind,
}

enum FieldKind {
    Normal(FieldType),
    SingleSelect(Vec<FieldOption>),
    Iteration {
        #[allow(dead_code)]
        duration: i64,
        #[allow(dead_code)]
        start_day: i64,
        iterations: Vec<FieldIteration>,
        completed_iterations: Vec<FieldIteration>,
    },
}

#[derive(Debug)]
#[allow(nonstandard_style, clippy::upper_case_acronyms)]
enum FieldType {
    ASSIGNEES,
    DATE,
    LABELS,
    LINKED_PULL_REQUESTS,
    MILESTONE,
    NUMBER,
    REPOSITORY,
    REVIEWERS,
    TEXT,
    TITLE,
    TRACKED_BY,
    TRACKS,
    Other(String),
}

impl FieldType {
    fn as_sql_type(&self) -> Option<DataType> {
        Some(match self {
            FieldType::DATE => DataType::Date,
            FieldType::NUMBER => DataType::Float,
            FieldType::TEXT => DataType::Text,
            FieldType::TITLE => DataType::Text,
            _ => None?,
        })
    }
}

struct FieldOption {
    id: String,
    name: String,
}

struct FieldIteration {
    id: String,
    title: String,
    duration: i64,
    start_date: String,
}

pub struct ProjectNextStorage {
    owner: String,
    project_number: i64,
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
                                Value::I64(*duration),
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
                                Value::I64(*duration),
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
                    kind: FieldKind::SingleSelect(options),
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

#[allow(warnings)]
mod generated {
    type Date = String;
    include!(concat!(env!("OUT_DIR"), "/list_fields.rs"));
    include!(concat!(env!("OUT_DIR"), "/list_items.rs"));
    include!(concat!(env!("OUT_DIR"), "/update_item_field.rs"));
}

impl ProjectNextStorage {
    pub fn new(owner: String, project_number: i64) -> Result<Self> {
        Ok(Self {
            owner,
            project_number,
            cache: Mutex::new(None),
        })
    }

    fn list_fields(&self) -> Result<(String, Vec<Field>)> {
        use generated::list_fields::*;
        type SingleSelectFieldOption =
            ProjectV2ProjectV2FieldsNodesOnProjectV2SingleSelectFieldOptions;
        impl From<SingleSelectFieldOption> for FieldOption {
            fn from(SingleSelectFieldOption { id, name }: SingleSelectFieldOption) -> Self {
                Self { id, name }
            }
        }
        type CompletedIteration =
            ProjectV2ProjectV2FieldsNodesOnProjectV2IterationFieldConfigurationCompletedIterations;
        impl From<CompletedIteration> for FieldIteration {
            fn from(
                CompletedIteration {
                    id,
                    title,
                    duration,
                    start_date,
                    ..
                }: CompletedIteration,
            ) -> Self {
                Self {
                    id,
                    title,
                    duration,
                    start_date,
                }
            }
        }
        type Iteration =
            ProjectV2ProjectV2FieldsNodesOnProjectV2IterationFieldConfigurationIterations;
        impl From<Iteration> for FieldIteration {
            fn from(
                Iteration {
                    id,
                    title,
                    duration,
                    start_date,
                    ..
                }: Iteration,
            ) -> Self {
                Self {
                    id,
                    title,
                    duration,
                    start_date,
                }
            }
        }
        impl From<ProjectV2FieldType> for FieldType {
            fn from(value: ProjectV2FieldType) -> Self {
                match value {
                    ProjectV2FieldType::ITERATION | ProjectV2FieldType::SINGLE_SELECT => {
                        unreachable!()
                    }
                    ProjectV2FieldType::ASSIGNEES => Self::ASSIGNEES,
                    ProjectV2FieldType::DATE => Self::DATE,
                    ProjectV2FieldType::LABELS => Self::LABELS,
                    ProjectV2FieldType::LINKED_PULL_REQUESTS => Self::LINKED_PULL_REQUESTS,
                    ProjectV2FieldType::MILESTONE => Self::MILESTONE,
                    ProjectV2FieldType::NUMBER => Self::NUMBER,
                    ProjectV2FieldType::REPOSITORY => Self::REPOSITORY,
                    ProjectV2FieldType::REVIEWERS => Self::REVIEWERS,
                    ProjectV2FieldType::TEXT => Self::TEXT,
                    ProjectV2FieldType::TITLE => Self::TITLE,
                    ProjectV2FieldType::TRACKED_BY => Self::TRACKED_BY,
                    ProjectV2FieldType::TRACKS => Self::TRACKS,
                    ProjectV2FieldType::Other(s) => Self::Other(s),
                }
            }
        }
        let query = include_str!("list_fields.graphql");
        let variables = Variables {
            owner: self.owner.clone(),
            project_number: self.project_number,
        };
        let resp: gh::GraphQLResponse<ResponseData> = gh::graphql(query, &variables)?;
        let project_next = resp
            .data
            .organization
            .and_then(|org| org.project_v2)
            .or_else(|| resp.data.user.and_then(|user| user.project_v2));
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
            .flatten()
            .flatten()
            .filter_map(|node| {
                use ProjectV2ProjectV2FieldsNodes::*;
                let field = match node {
                    ProjectV2Field(ProjectV2ProjectV2FieldsNodesOnProjectV2Field { id, name, data_type }) => {
                        if reserved_names.iter().any(|&rname| rname == name) {
                            return None;
                        } else {
                            return Some(Field { id, name, kind: FieldKind::Normal(data_type.into()) });
                        }
                    },
                    ProjectV2IterationField(ProjectV2ProjectV2FieldsNodesOnProjectV2IterationField {
                        id,
                        name,
                        configuration: ProjectV2ProjectV2FieldsNodesOnProjectV2IterationFieldConfiguration {
                            duration,
                        start_day,
                        iterations,
                        completed_iterations,
                        },
                        ..
                    }) => {
                        Field { id, name, kind:
                            FieldKind::Iteration {
                                duration,
                                start_day,
                                iterations: iterations.into_iter().map(Into::into).collect(),
                                completed_iterations: completed_iterations
                                    .into_iter()
                                    .map(Into::into)
                                    .collect(),
                            }
                        }
                    },
                    ProjectV2SingleSelectField(ProjectV2ProjectV2FieldsNodesOnProjectV2SingleSelectField {
                        id,
                        name,
                        options,
                        ..
                    }) => {
                        let options = options.into_iter().map(Into::into).collect();
                        Field { id, name, kind: FieldKind::SingleSelect(options) }
                    }
                };
                Some(field)
            })
            .collect();
        Ok((project_id, fields))
    }

    fn scan_items(&self, project_id: String, fields: &[Field]) -> Result<Vec<(String, Row)>> {
        use generated::list_items::*;
        trait IntoQuadRow {
            /// repo, issue number, assignees, labels
            fn into_row(self) -> (Value, Value, Value, Value);
        }
        impl IntoQuadRow for ListItemsNodeOnProjectV2ItemsNodesContent {
            fn into_row(self) -> (Value, Value, Value, Value) {
                match self {
                    ListItemsNodeOnProjectV2ItemsNodesContent::Issue(issue) => issue.into_row(),
                    ListItemsNodeOnProjectV2ItemsNodesContent::PullRequest(pr) => pr.into_row(),
                    ListItemsNodeOnProjectV2ItemsNodesContent::DraftIssue(draft) => {
                        draft.into_row()
                    }
                }
            }
        }
        macro_rules! impl_into_quad_rows {
            ($($t:tt),*) => {
                $(impl_into_quad_row!($t));*
            };
        }
        macro_rules! impl_into_quad_row {
            ($t:ident) => {
                impl IntoQuadRow for $t {
                    fn into_row(self) -> (Value, Value, Value, Value) {
                        let repo = self.repository.name_with_owner;
                        let assignees = self
                            .assignees
                            .nodes
                            .into_iter()
                            .flatten()
                            .flatten()
                            .map(|u| Value::Str(u.login))
                            .collect();
                        let labels = self
                            .labels
                            .into_iter()
                            .flat_map(|l| l.nodes)
                            .flatten()
                            .flatten()
                            .map(|l| Value::Str(l.name))
                            .collect();
                        (
                            Value::Str(repo),
                            Value::I64(self.number as i64),
                            Value::List(assignees),
                            Value::List(labels),
                        )
                    }
                }
            };
        }
        impl_into_quad_rows! {
            ListItemsNodeOnProjectV2ItemsNodesContentOnIssue,
            ListItemsNodeOnProjectV2ItemsNodesContentOnPullRequest
        }
        impl IntoQuadRow for ListItemsNodeOnProjectV2ItemsNodesContentOnDraftIssue {
            fn into_row(self) -> (Value, Value, Value, Value) {
                let assignees = self
                    .assignees
                    .nodes
                    .into_iter()
                    .flatten()
                    .flatten()
                    .map(|u| Value::Str(u.login))
                    .collect();
                (
                    Value::Null,
                    Value::Null,
                    Value::List(assignees),
                    Value::List(vec![]),
                )
            }
        }

        impl ListItemsNodeOnProjectV2ItemsNodesContent {
            fn title(&self) -> &str {
                match self {
                    ListItemsNodeOnProjectV2ItemsNodesContent::DraftIssue(d) => &d.title,
                    ListItemsNodeOnProjectV2ItemsNodesContent::Issue(i) => &i.title,
                    ListItemsNodeOnProjectV2ItemsNodesContent::PullRequest(p) => &p.title,
                }
            }
        }
        impl ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes {
            fn field(&self) -> &FieldFragment {
                match self {
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldDateValue(i) => &i.field,
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldIterationValue(i) => &i.field,
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldLabelValue(i) => &i.field,
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldMilestoneValue(i) => &i.field,
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldNumberValue(i) => &i.field,
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldPullRequestValue(i) => &i.field,
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldRepositoryValue(i) => &i.field,
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldReviewerValue(i) => &i.field,
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldSingleSelectValue(i) => &i.field,
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldTextValue(i) => &i.field,
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldUserValue(i) => &i.field,
                }
            }
            fn as_sql_value(&self) -> Option<Value> {
                match self {
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldDateValue(f) => f.date.as_ref().map(|s| Value::Str(s.to_owned())),
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldIterationValue(..) => unreachable!(),
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldLabelValue(f) => {
                        let l = f.labels.as_ref()?;
                        let names: Vec<_> = l.nodes.iter().flatten().flatten().map(|ls| Value::Str(ls.name.to_owned())).collect();
                        if names.is_empty() {
                            return None;
                        }
                        Some(Value::List(names))
                    }
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldMilestoneValue(f) => f.milestone.as_ref().map(|m| Value::Str(m.title.to_owned())),
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldNumberValue(f) => f.number.map(Value::F64),
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldPullRequestValue(f) => {
                        let l = f.pull_requests.as_ref()?;
                        let titles: Vec<_> = l.nodes.iter().flatten().flatten().map(|ls| Value::Str(ls.title.to_owned())).collect();
                        if titles.is_empty() {
                            return None;
                        }
                        Some(Value::List(titles))
                    }
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldRepositoryValue(f) => f.repository.as_ref().map(|re| Value::Str(re.name.to_owned())),
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldReviewerValue(f) => {
                        let l = f.reviewers.as_ref()?;
                        let logins: Vec<_> = l.nodes.iter().flatten().flatten().flat_map(|ls| ls.name()).map(|s| Value::Str(s.to_owned())).collect();
                        if logins.is_empty() {
                            return None;
                        }
                        Some(Value::List(logins))
                    }
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldSingleSelectValue(..) => unreachable!(),
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldTextValue(f) => f.text.as_ref().map(|s| Value::Str(s.to_owned())),
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodes::ProjectV2ItemFieldUserValue(f) => {
                        let l = f.users.as_ref()?;
                        let logins: Vec<_> = l.nodes.iter().flatten().flatten().map(|ls| Value::Str(ls.login.to_owned())).collect();
                        if logins.is_empty() {
                            return None;
                        }
                        Some(Value::List(logins))
                    }
                }
            }
            fn as_single_select(&self) -> Option<&ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodesOnProjectV2ItemFieldSingleSelectValue>{
                if let Self::ProjectV2ItemFieldSingleSelectValue(v) = self {
                    Some(v)
                } else {
                    None
                }
            }
            fn as_iteration(&self) -> Option<&ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodesOnProjectV2ItemFieldIterationValue>{
                if let Self::ProjectV2ItemFieldIterationValue(v) = self {
                    Some(v)
                } else {
                    None
                }
            }
        }
        impl FieldFragment {
            fn id(&self) -> &str {
                match self {
                    FieldFragment::ProjectV2Field(i) => &i.id,
                    FieldFragment::ProjectV2IterationField(i) => &i.id,
                    FieldFragment::ProjectV2SingleSelectField(i) => &i.id,
                }
            }
        }
        impl ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodesOnProjectV2ItemFieldReviewerValueReviewersNodes {
            fn name(&self) -> Option<&str> {
                match self {
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodesOnProjectV2ItemFieldReviewerValueReviewersNodes::Team(t) => Some(&t.name),
                    ListItemsNodeOnProjectV2ItemsNodesFieldValuesNodesOnProjectV2ItemFieldReviewerValueReviewersNodes::User(u) => Some(&u.login),
                    _ => None,
                }
            }
        }

        let query = include_str!("list_items.graphql");
        let mut items = vec![];
        let mut after = None;
        while {
            let variables = Variables {
                project_id: project_id.clone(),
                after: after.clone(),
            };
            let resp: GraphQLResponse<ResponseData> = gh::graphql(query, &variables)?;
            let Some(ListItemsNode::ProjectV2(ListItemsNodeOnProjectV2 { items: ListItemsNodeOnProjectV2Items { page_info, nodes } })) = resp.data.node else { unreachable!("the id can only be for projectV2") };
            items.extend(nodes.into_iter().flatten().flatten());
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
                let title = item
                    .content
                    .as_ref()
                    .map(ListItemsNodeOnProjectV2ItemsNodesContent::title)
                    .unwrap_or_default()
                    .to_string();
                let (repo, issue, assignees, labels) = match item.content {
                    Some(content) => content.into_row(),
                    None => (Value::Null, Value::Null, Value::Null, Value::Null),
                };
                let reserved_columns = [
                    Value::Str(key.clone()),
                    repo,
                    issue,
                    Value::Str(title),
                    assignees,
                    labels,
                ];
                let field_columns = fields.iter().map(|field| {
                    let value = item
                        .field_values
                        .nodes
                        .iter()
                        .flatten()
                        .flatten()
                        .find(|value| value.field().id() == field.id);
                    match value {
                        Some(value) => match &field.kind {
                            FieldKind::Normal(..) => match value.as_sql_value() {
                                Some(v) => v,
                                None => Value::Null,
                            },
                            FieldKind::SingleSelect(_) => {
                                if let Some(opt) = value.as_single_select().unwrap().name.as_ref() {
                                    Value::Str(opt.to_owned())
                                } else {
                                    Value::Null
                                }
                            }
                            FieldKind::Iteration {
                                iterations,
                                completed_iterations,
                                ..
                            } => {
                                let value = value.as_iteration().unwrap();
                                let title = &value.title;
                                if let Some(iter) = iterations
                                    .iter()
                                    .chain(completed_iterations.iter())
                                    .find(|iter| &iter.title == title)
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
                name: "start_date".to_string(),
                data_type: DataType::Text,
                options: vec![],
            },
            ColumnDef {
                name: "duration".to_string(),
                data_type: DataType::Int,
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

    fn fetch_data(&self) -> Result<Cache> {
        let (project_id, fields) = self.list_fields()?;
        let items = self.scan_items(project_id.clone(), &fields)?;
        Ok(Cache {
            project_id,
            fields,
            items,
        })
    }

    fn update_item_field(
        &self,
        project_id: String,
        item_id: String,
        field_id: String,
        value: ProjectV2FieldValue,
    ) -> Result<()> {
        let query = include_str!("./update_item_field.graphql");
        let variables = Variables {
            project_id,
            item_id,
            field_id,
            value,
        };
        let resp: GraphQLResponse<generated::update_item_field::ResponseData> =
            gh::graphql(query, &variables)?;
        if !resp.errors.is_empty() {
            return Err(anyhow::anyhow!("Error: {:?}", resp.errors));
        }
        Ok(())
    }

    fn delete_item_field(&self, project_id: String, item_id: String) -> Result<()> {
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
        let resp: GraphQLResponse<Response> = gh::graphql(query, &variables)?;
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Variables {
    pub project_id: String,
    pub item_id: String,
    pub field_id: String,
    pub value: ProjectV2FieldValue,
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ProjectV2FieldValue {
    #[serde(skip_serializing_if = "Option::is_none")]
    date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    iteration_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    number: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    single_select_option_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
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
                    let field = &cache.fields[field_idx];
                    let new_value_input = if !matches!(new_value, Value::Null) {
                        match &field.kind {
                            FieldKind::Normal(ty) => {
                                let Some(ty) = ty.as_sql_type() else {
                                    return Err((
                                        self,
                                        GlueSQLError::StorageMsg(format!("readonly column: {:?}", ty)),
                                    ));
                                };

                                fn into_update_input(
                                    ty: &DataType,
                                    new_value: &Value,
                                ) -> Option<ProjectV2FieldValue> {
                                    Some(match ty {
                                        DataType::Date => ProjectV2FieldValue {
                                            date: Some(match new_value {
                                                Value::Str(s) => s.to_owned(),
                                                Value::Date(d) => d.format("%Y-%m-%d").to_string(),
                                                _ => None?,
                                            }),
                                            ..Default::default()
                                        },
                                        DataType::Float => ProjectV2FieldValue {
                                            number: new_value
                                                .cast(&DataType::Float)
                                                .ok()
                                                .and_then(|v| (&v).try_into().ok()),
                                            ..Default::default()
                                        },
                                        DataType::Text => ProjectV2FieldValue {
                                            text: new_value
                                                .cast(&DataType::Text)
                                                .ok()
                                                .map(|v| v.into()),
                                            ..Default::default()
                                        },
                                        _ => None?,
                                    })
                                }

                                let Some(new_value_input) = into_update_input(&ty, new_value) else {
                                    return Err((
                                        self,
                                        GlueSQLError::Value(ValueError::IncompatibleDataType {
                                            data_type: ty,
                                            value: new_value.clone(),
                                        }),
                                    ));
                                };
                                new_value_input
                            }
                            FieldKind::SingleSelect(options) => {
                                let new_str: String = new_value.into();
                                if let Some(opt) = options.iter().find(|opt| opt.name == new_str) {
                                    ProjectV2FieldValue {
                                        single_select_option_id: Some(opt.id.to_owned()),
                                        ..Default::default()
                                    }
                                } else {
                                    return Err((
                                        self,
                                        GlueSQLError::Value(ValueError::ImpossibleCast),
                                    ));
                                }
                            }
                            FieldKind::Iteration { .. } => {
                                let new_str: String = new_value.into();
                                ProjectV2FieldValue {
                                    iteration_id: Some(new_str.to_owned()),
                                    ..Default::default()
                                }
                            }
                        }
                    } else {
                        Default::default()
                    };
                    if let Err(e) = self.update_item_field(
                        cache.project_id.clone(),
                        item_id.clone(),
                        field.id.clone(),
                        new_value_input,
                    ) {
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
            if let Err(e) = self.delete_item_field(cache.project_id.clone(), item_id) {
                return Err((self, GlueSQLError::Storage(e.into())));
            }
        }
        Ok((self, ()))
    }
}

impl GStore<String> for ProjectNextStorage {}
impl GStoreMut<String> for ProjectNextStorage {}
