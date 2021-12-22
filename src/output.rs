use std::{fmt, io, str::FromStr};

use anyhow::{anyhow, Error, Result};
use gluesql::prelude::Value;
use unicode_width::UnicodeWidthStr;

#[derive(Debug)]
pub enum Format {
    Table,
    Json,
}

impl FromStr for Format {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "t" | "table" => Ok(Format::Table),
            "j" | "json" => Ok(Format::Json),
            other => Err(anyhow!("Unknown format: {}", other)),
        }
    }
}

impl Format {
    pub fn print<W: io::Write>(
        &self,
        w: W,
        labels: Vec<String>,
        rows: Vec<Vec<Value>>,
    ) -> Result<()> {
        match self {
            Format::Table => print_as_table(w, labels, rows),
            Format::Json => print_as_json(w, labels, rows),
        }
    }
}

fn print_as_table<W: io::Write>(
    mut w: W,
    labels: Vec<String>,
    rows: Vec<Vec<Value>>,
) -> Result<()> {
    let rows = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|value| {
                    let mut s = String::new();
                    print_value_in_table(&mut s, &value)?;
                    let width = s.width();
                    Ok((s, width))
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    let label_widths = labels.iter().map(|label| label.width()).collect::<Vec<_>>();
    let column_widths = rows.iter().fold(label_widths, |mut widths, row| {
        for (max_width, (_, width)) in widths.iter_mut().zip(row) {
            *max_width = (*max_width).max(*width);
        }
        widths
    });
    w.write_all(b"| ")?;
    for (max_width, label) in column_widths.iter().zip(labels) {
        write!(w, "{}{:pad$} | ", label, "", pad = max_width - label.width())?;
    }
    w.write_all(b"\n")?;
    for row in rows {
        w.write_all(b"| ")?;
        for (max_width, (value, width)) in column_widths.iter().zip(row) {
            write!(w, "{}{:pad$} | ", value, "", pad = max_width - width)?;
        }
        w.write_all(b"\n")?;
    }
    Ok(())
}

fn print_value_in_table<W: fmt::Write>(fmt: &mut W, value: &Value) -> Result<(), fmt::Error> {
    match value {
        Value::Bool(b) => write!(fmt, "{}", *b),
        Value::I64(i) => write!(fmt, "{}", *i),
        Value::F64(f) => write!(fmt, "{}", *f),
        Value::Str(s) => write!(fmt, "{}", s),
        Value::Date(dt) => write!(fmt, "{}", *dt),
        Value::Timestamp(ts) => write!(fmt, "{}", *ts),
        Value::Time(tm) => write!(fmt, "{}", *tm),
        Value::Interval(_) => unimplemented!(),
        Value::Uuid(_) => unimplemented!(),
        Value::Map(_) => unimplemented!(),
        Value::List(list) => {
            if let [head, tail @ ..] = list.as_slice() {
                print_value_in_table(fmt, head)?;
                for elem in tail {
                    write!(fmt, ", ")?;
                    print_value_in_table(fmt, elem)?;
                }
            }
            Ok(())
        }
        Value::Null => write!(fmt, ""),
    }
}

fn print_as_json<W: io::Write>(mut w: W, labels: Vec<String>, rows: Vec<Vec<Value>>) -> Result<()> {
    for row in rows {
        let mut row_map = serde_json::Map::with_capacity(labels.len());
        for (label, value) in labels.iter().zip(row) {
            let json_value = into_json_value(value);
            row_map.insert(label.clone(), json_value);
        }
        serde_json::to_writer(&mut w, &row_map)?;
        writeln!(&mut w)?;
    }
    Ok(())
}

fn into_json_value(value: Value) -> serde_json::Value {
    match value {
        Value::Bool(b) => b.into(),
        Value::I64(i) => i.into(),
        Value::F64(f) => f.into(),
        Value::Str(s) => s.into(),
        Value::Date(dt) => format!("{}", dt).into(),
        Value::Timestamp(ts) => format!("{}", ts).into(),
        Value::Time(tm) => format!("{}", tm).into(),
        Value::Interval(_) => unimplemented!(),
        Value::Uuid(_) => unimplemented!(),
        Value::Map(_) => unimplemented!(),
        Value::List(list) => list
            .into_iter()
            .map(into_json_value)
            .collect::<Vec<_>>()
            .into(),
        Value::Null => serde_json::Value::Null,
    }
}
