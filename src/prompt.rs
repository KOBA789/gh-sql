use std::{
    fmt::Debug,
    io::{BufWriter, Write},
};

use anyhow::Result;
use gluesql::{
    executor::Payload,
    prelude::{Glue, Value},
    store::{GStore, GStoreMut},
};
use rustyline::{error::ReadlineError, Editor, Helper};

use crate::output::Format;

pub struct Opt {
    pub format: Format,
}

pub struct Prompt<K, S, H>
where
    K: Debug,
    S: GStore<K> + GStoreMut<K>,
    H: Helper,
{
    opt: Opt,
    glue: Glue<K, S>,
    rl: Editor<H>,
}

impl<K, S, H> Prompt<K, S, H>
where
    K: Debug,
    S: GStore<K> + GStoreMut<K>,
    H: Helper,
{
    pub fn new(opt: Opt, glue: Glue<K, S>, rl: Editor<H>) -> Self {
        Self { opt, rl, glue }
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            if let Err(e) = self.readline() {
                match e.downcast::<ReadlineError>() {
                    Ok(ReadlineError::Interrupted) | Ok(ReadlineError::Eof) => {
                        return Ok(());
                    }
                    Ok(e) => return Err(e.into()),
                    Err(e) => return Err(e),
                }
            }
        }
    }

    fn readline(&mut self) -> Result<()> {
        let line = self.rl.readline("ghsql> ")?;
        if line.is_empty() {
            return Ok(());
        }
        self.rl.add_history_entry(line.as_str());
        let output = self.glue.execute(&line);
        match output {
            Ok(Payload::Select { labels, rows }) => {
                print(&self.opt.format, labels, rows)?;
            }
            Ok(_) => {}
            Err(err) => {
                eprintln!("SQL execution error: {:?}", err);
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
fn print(format: &Format, labels: Vec<String>, rows: Vec<Vec<Value>>) -> Result<()> {
    use std::process::{Command, Stdio};
    let mut pager = Command::new("less")
        .args(["-FS"])
        .stdin(Stdio::piped())
        .spawn()?;
    let pipe = pager.stdin.as_mut().unwrap();
    let mut pipe = BufWriter::new(pipe);
    format.print(&mut pipe, labels, rows)?;
    pipe.flush()?;
    drop(pipe);
    pager.wait()?;
    Ok(())
}

#[cfg(windows)]
fn print(format: &Format, labels: Vec<String>, rows: Vec<Vec<Value>>) -> Result<()> {
    let stdout = std::io::stdout();
    let stdout = stdout.lock();
    let mut stdout = BufWriter::new(stdout);
    format.print(&mut stdout, labels, rows)?;
    stdout.flush()?;
    Ok(())
}
