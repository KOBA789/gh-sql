use std::{
    fmt::Debug,
    io::{BufWriter, Write},
};

use anyhow::Result;
use futures::executor::block_on;
use gluesql::{
    executor::Payload,
    prelude::{plan, translate, Glue, Value},
    sqlparser::tokenizer::Token,
    store::{GStore, GStoreMut},
};
use rustyline::{error::ReadlineError, Editor, Helper};

use crate::output::{error_to_string, Format};

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
    input_buf: String,
    tokens_buf: Vec<Token>,
}

impl<K, S, H> Prompt<K, S, H>
where
    K: Debug,
    S: GStore<K> + GStoreMut<K>,
    H: Helper,
{
    pub fn new(opt: Opt, glue: Glue<K, S>, rl: Editor<H>) -> Self {
        Self {
            opt,
            rl,
            glue,
            input_buf: String::new(),
            tokens_buf: vec![],
        }
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            if let Err(e) = self.readline() {
                match e.downcast::<ReadlineError>() {
                    Ok(ReadlineError::Interrupted) => {
                        self.input_buf = String::new();
                        self.tokens_buf = vec![];
                    }
                    Ok(ReadlineError::Eof) => {
                        return Ok(());
                    }
                    Ok(e) => return Err(e.into()),
                    Err(e) => return Err(e),
                }
            }
        }
    }

    fn is_buffer_empty(&self) -> bool {
        self.input_buf.is_empty() && self.tokens_buf.is_empty()
    }

    fn prompt(&self) -> &'static str {
        if self.is_buffer_empty() {
            "ghsql> "
        } else {
            "    -> "
        }
    }

    fn readline(&mut self) -> Result<()> {
        let line = self.rl.readline(self.prompt())?;
        if line.is_empty() {
            return Ok(());
        }
        self.rl.add_history_entry(line.as_str());
        self.input_buf.push_str(&line);
        self.input_buf.push('\n');
        let dialect = gluesql::sqlparser::dialect::GenericDialect {};
        let mut tokenizer =
            gluesql::sqlparser::tokenizer::Tokenizer::new(&dialect, &self.input_buf);
        if let Ok(new_tokens) = tokenizer.tokenize() {
            self.tokens_buf.extend(new_tokens);
            self.input_buf = String::new();
        }
        let tokens = if let Some(pos) = self.tokens_buf.iter().position(|t| t == &Token::SemiColon)
        {
            let ws_len = self.tokens_buf[pos + 1..]
                .iter()
                .take_while(|t| matches!(t, Token::Whitespace(_)))
                .count();
            self.tokens_buf.drain(..=pos + ws_len).collect()
        } else {
            return Ok(());
        };
        let mut parser = gluesql::sqlparser::parser::Parser::new(tokens, &dialect);
        let statement = match parser.parse_statement() {
            Ok(statement) => statement,
            Err(e) => {
                eprintln!("Syntax Error: {}", e);
                return Ok(());
            }
        };
        let output = translate(&statement)
            .and_then(|statement| block_on(plan(self.glue.storage.as_ref().unwrap(), statement)))
            .and_then(|plan| self.glue.execute_stmt(plan));
        match output {
            Ok(Payload::Select { labels, rows }) => {
                print(&self.opt.format, labels, rows)?;
            }
            Ok(_) => {}
            Err(err) => {
                eprintln!("SQL execution error: {}", error_to_string(err));
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
