//! Single-client, read-only comparison of a nonunique document equality query.
#[path = "support/nonunique_query.rs"]
mod nonunique_query;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    nonunique_query::run(false)
}
