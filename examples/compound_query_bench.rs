//! Single-client, read-only comparison of a complete compound equality query.
#[path = "support/compound_query.rs"]
mod compound_query;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    compound_query::run(false)
}
