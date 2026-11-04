//! Single-client, read-only comparison of a two-value membership query.
#[path = "support/nonunique_query.rs"]
mod nonunique_query;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    nonunique_query::run(true)
}
