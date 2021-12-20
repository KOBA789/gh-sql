use std::io::{BufWriter, Write};
use std::process::{Stdio, Command};

use anyhow::Result;
use gluesql::prelude::*;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use structopt::StructOpt;

mod github;
mod storage;

#[derive(Debug, StructOpt)]
#[structopt(name = "ghsql")]
struct Opt {
    #[structopt(long, env)]
    github_token: String,
    #[structopt(name = "OWNER")]
    owner: String,
    #[structopt(name = "PROJECT_NUMBER")]
    project_number: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();
    let reqwest = reqwest::Client::new();
    let github = github::Client::new("https://api.github.com".parse().unwrap(), opt.github_token, reqwest);
    let storage = storage::ProjectNextStorage::new(github, opt.owner, opt.project_number).await?;
    let mut glue = Glue::new(storage);

    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline("ghsql> ");
        match readline {
            Ok(line) => {
                if line.is_empty() {
                    continue;
                }
                rl.add_history_entry(line.as_str());
                let output = glue.execute(&line);
                match output {
                    Ok(Payload::Select { labels, rows }) => {
                        let mut pager = Command::new("sh")
                            .args(["-c", "column -s '\t' -n -t | less -FS"])
                            .stdin(Stdio::piped())
                            .spawn()?;
                        let pipe = pager.stdin.as_mut().unwrap();
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
                                fn print_value<W: Write>(w: &mut W, value: &Value) -> std::io::Result<()> {
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
                                            if let [head, tail @ .. ] = list.as_slice() {
                                                print_value(w, head)?;
                                                for elem in tail {
                                                    write!(w, ", ")?;
                                                    print_value(w, elem)?;
                                                }
                                            }
                                            Ok(())
                                        },
                                        Value::Null => write!(w, ""),
                                    }
                                }
                                print_value(&mut pipe, &value)?;
                                pipe.write_all(b"\t| ")?;
                            }
                            pipe.write_all(b"\n")?;
                        }
                        pipe.flush()?;
                        drop(pipe);
                        pager.wait()?;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("Error: {:?}", err);
                    }
                }
            }
            Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    Ok(())
}
