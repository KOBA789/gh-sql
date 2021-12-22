use std::{
    io::{BufWriter, Write},
    process::{Command, Stdio},
    str::FromStr,
};

use anyhow::{anyhow, Error, Result};
use gluesql::prelude::Value;

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
    pub fn print<W: Write>(&self, w: W, labels: Vec<String>, rows: Vec<Vec<Value>>) -> Result<()> {
        match self {
            Format::Table => print_as_table(w, labels, rows),
            Format::Json => print_as_json(w, labels, rows),
        }
    }
}

fn print_as_table<W: Write>(mut w: W, labels: Vec<String>, rows: Vec<Vec<Value>>) -> Result<()> {
    let mut column = Command::new("column")
        .args(["-s", "\t", "-n", "-t"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let pipe = column.stdin.as_mut().expect("stdin is piped");
    let mut pipe = BufWriter::new(pipe);
    pipe.write_all(b"| ")?;
    for label in labels {
        pipe.write_all(label.as_bytes())?;
        pipe.write_all(b"\t| ")?;
    }
    pipe.write_all(b"\n")?;
    for row in rows {
        pipe.write_all(b"| ")?;
        for value in row {
            print_value_in_table(&mut pipe, &value)?;
            pipe.write_all(b"\t| ")?;
        }
        pipe.write_all(b"\n")?;
    }
    pipe.flush()?;
    drop(pipe);
    let output = column.wait_with_output()?;
    w.write_all(&output.stdout)?;
    Ok(())
}

fn print_value_in_table<W: Write>(w: &mut W, value: &Value) -> std::io::Result<()> {
    match value {
        Value::Bool(b) => write!(w, "{}", *b),
        Value::I64(i) => write!(w, "{}", *i),
        Value::F64(f) => write!(w, "{}", *f),
        Value::Str(s) => write!(w, "{}", s),
        Value::Date(dt) => write!(w, "{}", *dt),
        Value::Timestamp(ts) => write!(w, "{}", *ts),
        Value::Time(tm) => write!(w, "{}", *tm),
        Value::Interval(_) => unimplemented!(),
        Value::Uuid(_) => unimplemented!(),
        Value::Map(_) => unimplemented!(),
        Value::List(list) => {
            if let [head, tail @ ..] = list.as_slice() {
                print_value_in_table(w, head)?;
                for elem in tail {
                    write!(w, ", ")?;
                    print_value_in_table(w, elem)?;
                }
            }
            Ok(())
        }
        Value::Null => write!(w, ""),
    }
}

fn print_as_json<W: Write>(mut w: W, labels: Vec<String>, rows: Vec<Vec<Value>>) -> Result<()> {
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
