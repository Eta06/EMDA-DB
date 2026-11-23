use database_engine::{
    CollectionSchema, DocumentDatabase, DocumentFilter, DocumentQuery, DocumentQueryAccess,
    DocumentSort, IndexDefinition, Store,
};
use serde_json::json;
use std::path::Path;
use std::time::Instant;

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let store = Store::open(Path::new(&args[2])).unwrap();
    let db = DocumentDatabase::new(&store);
    if args[1] == "prepare" {
        db.create_collection("c", CollectionSchema::default())
            .unwrap();
        db.create_index(
            "c",
            IndexDefinition {
                name: "group".into(),
                fields: vec![vec!["group".into()]],
                unique: false,
                partial: None,
            },
        )
        .unwrap();
        let payload = vec![0_u8; 8192];
        for start in (0..512).step_by(16) {
            db.transaction(|tx| {
                for id in start..start + 16 {
                    let rank = (id * 37) % 512;
                    tx.insert(
                        "c",
                        &format!("id{id:04}"),
                        &json!({
                            "group":"blue",
                            "rank":rank,
                            "payload":payload,
                            "view":{"label":rank}
                        }),
                    )?;
                }
                Ok(())
            })
            .unwrap();
        }
        return;
    }
    let query = DocumentQuery {
        filter: DocumentFilter::Equal {
            path: vec!["group".into()],
            value: json!("blue"),
        },
        sort: Some(DocumentSort {
            path: vec!["rank".into()],
            descending: false,
        }),
        projection: Some(vec![vec!["view".into(), "label".into()]]),
        offset: 420,
        limit: 8,
        max_scanned: 512,
    };
    let start = Instant::now();
    let result = if args[1] == "planned" {
        let planned = db.query_planned("c", &query).unwrap();
        assert_eq!(
            planned.access,
            DocumentQueryAccess::EqualityIndex {
                index: "group".into()
            }
        );
        planned.result
    } else {
        assert_eq!(args[1], "scan");
        db.query("c", &query).unwrap()
    };
    let elapsed = start.elapsed();
    assert_eq!(result.plan.scanned, 512);
    assert_eq!(result.plan.matched, 512);
    assert_eq!(result.documents.len(), 8);
    for (row, rank) in result.documents.into_iter().zip(420..428) {
        assert_eq!(row.document, json!({"view":{"label":rank}}));
        let id = (0..512).find(|id| (id * 37) % 512 == rank).unwrap();
        assert_eq!(row.id, format!("id{id:04}"));
    }
    println!("elapsed_us={} verified=true", elapsed.as_micros());
}
