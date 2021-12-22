use std::{
    fmt::Debug,
    io::{BufWriter, Write},
    process::{Command, Stdio},
};

use anyhow::Result;
use gluesql::{
    executor::Payload,
    prelude::Glue,
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
                let mut pager = Command::new("less")
                    .args(["-FS"])
                    .stdin(Stdio::piped())
                    .spawn()?;
                let pipe = pager.stdin.as_mut().unwrap();
                let mut pipe = BufWriter::new(pipe);
                self.opt.format.print(&mut pipe, labels, rows)?;
                pipe.flush()?;
                drop(pipe);
                pager.wait()?;
            }
            Ok(_) => {}
            Err(err) => {
                eprintln!("SQL execution error: {:?}", err);
            }
        }
        Ok(())
    }
}
