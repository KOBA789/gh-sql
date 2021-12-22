use std::{
    fmt::Debug,
    io::{BufWriter, Write},
};

use anyhow::Result;
use gluesql::{
    executor::Payload,
    prelude::Glue,
    store::{GStore, GStoreMut},
};

use crate::output::Format;

pub struct Opt {
    pub format: Format,
    pub statement: String,
}

pub struct Batch<K, S>
where
    K: Debug,
    S: GStore<K> + GStoreMut<K>,
{
    opt: Opt,
    glue: Glue<K, S>,
}

impl<K, S> Batch<K, S>
where
    K: Debug,
    S: GStore<K> + GStoreMut<K>,
{
    pub fn new(opt: Opt, glue: Glue<K, S>) -> Self {
        Self { opt, glue }
    }

    pub fn run(&mut self) -> Result<()> {
        let output = self.glue.execute(&self.opt.statement);
        match output {
            Ok(Payload::Select { labels, rows }) => {
                let stdout = std::io::stdout();
                let stdout = stdout.lock();
                let mut stdout = BufWriter::new(stdout);
                self.opt.format.print(&mut stdout, labels, rows)?;
                stdout.flush()?;
                drop(stdout);
            }
            Ok(_) => {}
            Err(err) => {
                eprintln!("SQL execution error: {:?}", err);
            }
        }
        Ok(())
    }
}
