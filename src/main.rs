use anyhow::Result;
use structopt::StructOpt;

mod batch;
mod gh;
mod output;
mod prompt;
mod storage;

#[derive(Debug, StructOpt)]
#[structopt(name = "ghsql")]
struct Opt {
    #[structopt(name = "OWNER")]
    owner: String,
    #[structopt(name = "PROJECT_NUMBER")]
    project_number: u32,
    #[structopt(short, long, help = "SQL statement to execute")]
    execute: Option<String>,
    #[structopt(
        short,
        long,
        default_value = "table",
        help = "\"table\", \"json\" or these initial"
    )]
    output: output::Format,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    let storage = storage::ProjectNextStorage::new(opt.owner, opt.project_number)?;
    let glue = gluesql::prelude::Glue::new(storage);

    if let Some(statement) = opt.execute {
        let batch_opt = batch::Opt { format: opt.output, statement };
        let mut batch = batch::Batch::new(batch_opt, glue);
        batch.run()
    } else {
        let prompt_opt = prompt::Opt { format: opt.output };
        let rl = rustyline::Editor::<()>::new();
        let mut prompt = prompt::Prompt::new(prompt_opt, glue, rl);
        prompt.run()
    }
}
