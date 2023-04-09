use std::{
    env,
    fs::File,
    io::{self, Write},
    path::Path,
    process::Command,
};

use graphql_client_codegen::{
    generate_module_token_stream, CodegenMode, GraphQLClientCodegenOptions,
};
use syn::Token;

fn main() {
    // download it from https://docs.github.com/public/schema.docs.graphql
    let schema_path = "schema.docs.graphql".to_string();
    for file_name in [
        // "delete_item",
        // "list_items",
        "list_fields",
        // "update_item_field",
    ] {
        let mut options = GraphQLClientCodegenOptions::new(CodegenMode::Cli);
        options.set_module_visibility(
            syn::VisPublic {
                pub_token: <Token![pub]>::default(),
            }
            .into(),
        );
        let gen = generate_module_token_stream(
            format!("src/{file_name}.graphql").into(),
            Path::new(&schema_path),
            options,
        )
        .unwrap();

        let generated_code = format!("{gen}");

        let dest_file_path = format!("{}/{file_name}.rs", env::var("OUT_DIR").unwrap());

        let mut file = File::create(&dest_file_path).unwrap();
        write!(file, "{}", generated_code).unwrap();

        let output = Command::new("rustfmt")
            .arg(dest_file_path)
            .output()
            .unwrap();
        io::stderr().write_all(&output.stderr).unwrap();
        let status = output.status;
        if !status.success() {
            panic!("rustfmt error: {status}")
        }
    }
}
