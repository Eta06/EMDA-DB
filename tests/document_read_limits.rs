use database_engine::retained_base_v2::Limits;
use database_engine::retained_candidate::{
    AuthorizedRetainedStore, FileRetainedAuthority, IndexLimits, ManagedRetainedStore,
    OverlayLimits, RetainedAuthority, RetainedStoreLimits, SuffixLimits,
};
use database_engine::{
    AggregateStatsRequest, CheckpointStore, CheckpointView, CollectionSchema, Comparison,
    DocumentDatabase, DocumentFilter, DocumentQuery, DocumentReadLimits, DocumentRequestLimits,
    DocumentSnapshot, DocumentTransactionLimits, DocumentTransactionMode, IndexDefinition,
    IndexQuery, IndexValue, RetainedExportLimits, Store, TransactionFailure,
};
use serde_json::json;
use std::io;

mod support;
use support::retry_lock;

fn is_read_limit(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::InvalidInput
        && error.to_string() == "document transaction read limit exceeded"
}

fn check_limits(db: &DocumentDatabase<'_>) {
    let mode = DocumentTransactionMode::ReadSet;
    let limits = |units, bytes| DocumentReadLimits {
        max_read_units: units,
        max_read_bytes: bytes,
    };

    db.transaction_with_read_limits(limits(2, 8), mode, None, None, |tx| {
        assert_eq!(tx.get_raw(b"key")?, Some(b"data".to_vec()));
        assert_eq!(tx.get_raw(b"z")?, None);
        Ok(())
    })
    .unwrap();

    db.transaction_with_limits(
        DocumentTransactionLimits {
            reads: limits(2, 8),
            max_validation_items: 0,
        },
        mode,
        None,
        None,
        |tx| {
            assert_eq!(tx.get_raw(b"key")?, Some(b"data".to_vec()));
            assert!(tx.get_raw(b"z")?.is_none());
            Ok(())
        },
    )
    .unwrap();

    let outcome = db.transaction_with_limits(
        DocumentTransactionLimits {
            reads: limits(1, 8),
            max_validation_items: 0,
        },
        mode,
        None,
        None,
        |tx| {
            tx.put_raw(b"receipt", b"not committed")?;
            assert_eq!(tx.get_raw(b"key")?, Some(b"data".to_vec()));
            assert!(is_read_limit(&tx.get_raw(b"z").unwrap_err()));
            Ok(())
        },
    );
    assert!(matches!(outcome, Err(TransactionFailure::Callback(error)) if is_read_limit(&error)));

    let token = database_engine::CancellationToken::new();
    token.cancel();
    assert!(matches!(
        db.transaction_with_limits(
            DocumentTransactionLimits {
                reads: limits(0, 0),
                max_validation_items: 0
            },
            mode,
            None,
            Some(&token),
            |_| -> io::Result<()> { panic!("cancelled callback ran") },
        ),
        Err(TransactionFailure::Cancelled)
    ));

    for limits in [limits(2, 6), limits(0, 8)] {
        let result = db.transaction_with_read_limits(limits, mode, None, None, |tx| {
            tx.put_raw(b"receipt", b"not committed")?;
            assert!(is_read_limit(&tx.get_raw(b"key").unwrap_err()));
            assert!(is_read_limit(&tx.get_raw(b"z").unwrap_err()));
            Ok(())
        });
        assert!(
            matches!(result, Err(TransactionFailure::Callback(error)) if is_read_limit(&error))
        );
    }

    let page = IndexQuery {
        limit: 1,
        ..Default::default()
    };
    let result = db.transaction_with_read_limits(limits(4, usize::MAX), mode, None, None, |tx| {
        let first = tx.query_index("c", "score", &page)?;
        assert_eq!(first.documents[0].id, "a");
        let next = IndexQuery {
            after: first.next,
            ..page.clone()
        };
        assert!(is_read_limit(
            &tx.query_index("c", "score", &next).unwrap_err()
        ));
        Ok(())
    });
    assert!(matches!(result, Err(TransactionFailure::Callback(error)) if is_read_limit(&error)));

    let result = db.transaction_with_read_limits(limits(3, usize::MAX), mode, None, None, |tx| {
        tx.put_raw(b"receipt", b"not committed")?;
        let query = IndexQuery {
            limit: 2,
            ..Default::default()
        };
        // Metadata, the page request, and one index row fit. Reject the
        // whole page when admission of its second index row fails.
        assert!(is_read_limit(
            &tx.query_index("c", "score", &query).unwrap_err()
        ));
        Ok(())
    });
    assert!(matches!(result, Err(TransactionFailure::Callback(error)) if is_read_limit(&error)));

    let empty = IndexQuery {
        equal: vec![IndexValue::Integer(9)],
        ..Default::default()
    };
    db.transaction_with_read_limits(limits(2, usize::MAX), mode, None, None, |tx| {
        assert!(tx.query_index("c", "score", &empty)?.documents.is_empty());
        Ok(())
    })
    .unwrap();

    // One metadata read, two page requests (including EOF) and two
    // document rows fit, including when the planner chooses the scan.
    let full_scan = DocumentQuery::default();
    let stats_scan = AggregateStatsRequest {
        filter: DocumentFilter::All,
        group_by: None,
        fields: vec![vec!["score".into()]],
        max_scanned: 4,
    };
    for planned in [false, true] {
        db.transaction_with_read_limits(limits(5, usize::MAX), mode, None, None, |tx| {
            let result = if planned {
                tx.query_planned("c", &full_scan)?.result
            } else {
                tx.query("c", &full_scan)?
            };
            assert_eq!(result.plan.returned, 2);
            Ok(())
        })
        .unwrap();
        db.transaction_with_read_limits(limits(5, usize::MAX), mode, None, None, |tx| {
            let result = if planned {
                tx.aggregate_stats_planned("c", &stats_scan)?.result
            } else {
                tx.aggregate_stats("c", &stats_scan)?
            };
            assert_eq!(result.groups[0].count, 2);
            assert_eq!(result.groups[0].fields[0].sum, 3);
            Ok(())
        })
        .unwrap();
    }

    let range = DocumentFilter::Compare {
        path: vec!["score".into()],
        comparison: Comparison::GreaterOrEqual,
        value: IndexValue::Integer(1),
    };
    for aggregate in [false, true] {
        // Metadata, the page, two index rows and one document fit. The
        // lookahead row proves a candidate overrun without spending a
        // sixth unit fetching its document or exhausting the transaction.
        db.transaction_with_read_limits(limits(5, usize::MAX), mode, None, None, |tx| {
            let error = if aggregate {
                tx.aggregate_stats_planned(
                    "c",
                    &AggregateStatsRequest {
                        filter: range.clone(),
                        group_by: None,
                        fields: vec![vec!["score".into()]],
                        max_scanned: 1,
                    },
                )
                .unwrap_err()
            } else {
                tx.query_planned(
                    "c",
                    &DocumentQuery {
                        filter: range.clone(),
                        max_scanned: 1,
                        ..Default::default()
                    },
                )
                .unwrap_err()
            };
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(
                error.to_string(),
                "document scan exceeds the operation limit"
            );
            Ok(())
        })
        .unwrap();
    }

    let stats = AggregateStatsRequest {
        filter: DocumentFilter::In {
            path: vec!["score".into()],
            values: vec![json!(1), json!(2)],
        },
        group_by: None,
        fields: vec![vec!["score".into()]],
        max_scanned: 4,
    };
    db.transaction_with_read_limits(limits(7, usize::MAX), mode, None, None, |tx| {
        let result = tx.aggregate_stats_planned("c", &stats)?.result;
        assert_eq!(result.groups[0].count, 2);
        assert_eq!(result.groups[0].fields[0].sum, 3);
        Ok(())
    })
    .unwrap();
    let result = db.transaction_with_read_limits(limits(6, usize::MAX), mode, None, None, |tx| {
        tx.put_raw(b"receipt", b"not committed")?;
        assert!(is_read_limit(
            &tx.aggregate_stats_planned("c", &stats).unwrap_err()
        ));
        Ok(())
    });
    assert!(matches!(result, Err(TransactionFailure::Callback(error)) if is_read_limit(&error)));
    assert_eq!(db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(), None);
}

#[test]
fn document_validation_limit_rejects_before_document_and_receipt_append() {
    for checkpoint_backed in [false, true] {
        for (limit, conflict) in [(1, false), (2, false), (2, true)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store");
            let base = dir.path().join("checkpoint");
            let ordinary = Store::open(&path).unwrap();
            let documents = DocumentDatabase::new(&ordinary);
            documents
                .create_collection("c", CollectionSchema::default())
                .unwrap();
            documents
                .insert("c", "a", &json!({"state":"initial"}))
                .unwrap();
            let digest = ordinary.checkpoint_to(&base).unwrap();
            let exercise = |db: &DocumentDatabase<'_>| {
                let source = path.join("records.log");
                let mut before_commit = Vec::new();
                let outcome = db.transaction_with_limits(
                    DocumentTransactionLimits {
                        reads: DocumentReadLimits {
                            max_read_units: 8,
                            max_read_bytes: 16 * 1024,
                        },
                        max_validation_items: limit,
                    },
                    DocumentTransactionMode::ReadSetFifo,
                    None,
                    None,
                    |tx| {
                        tx.replace("c", "a", &json!({"state":"staged"}), false)?;
                        tx.put_raw(b"receipt", b"staged")?;
                        db.transaction(|other| {
                            if conflict {
                                other.replace("c", "a", &json!({"state":"intervening"}), false)?;
                            }
                            other.put_raw(b"independent", b"kept")
                        })?;
                        before_commit = std::fs::read(&source)?;
                        Ok(())
                    },
                );
                if conflict || limit == 1 {
                    let Err(TransactionFailure::Rejected(error)) = outcome else {
                        panic!("expected pre-append rejection: {outcome:?}");
                    };
                    assert_eq!(database_engine::is_transaction_conflict(&error), conflict);
                    if !conflict {
                        assert!(
                            error
                                .to_string()
                                .ends_with("cumulative validation limit exceeded")
                        );
                    }
                    assert_eq!(std::fs::read(&source).unwrap(), before_commit);
                } else {
                    outcome.unwrap();
                }
            };
            let budget = if checkpoint_backed {
                drop(ordinary);
                let store = retry_lock(|| CheckpointStore::open(&path, &base, &digest)).unwrap();
                exercise(&DocumentDatabase::with_checkpoint_store(&store));
                store.transaction_budget_status().unwrap()
            } else {
                exercise(&documents);
                let status = ordinary.transaction_budget_status().unwrap();
                drop(ordinary);
                status
            };
            assert_eq!(budget.active_transactions, 0);
            assert_eq!(budget.reserved_bytes, 0);
            let store = retry_lock(|| Store::open(&path)).unwrap();
            let db = DocumentDatabase::new(&store);
            let state = if conflict {
                "intervening"
            } else if limit == 1 {
                "initial"
            } else {
                "staged"
            };
            assert_eq!(db.get("c", "a").unwrap(), Some(json!({"state":state})));
            assert_eq!(
                store.get(b"receipt").unwrap(),
                (!conflict && limit == 2).then(|| b"staged".to_vec())
            );
            assert_eq!(
                store.get(b"independent").unwrap().as_deref(),
                Some(b"kept".as_slice())
            );
        }
    }
}

fn seed_indexed_documents(db: &DocumentDatabase<'_>) {
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "score".into(),
            fields: vec![vec!["score".into()]],
            unique: false,
            partial: None,
        },
    )
    .unwrap();
    db.insert("c", "a", &json!({"score": 1})).unwrap();
    db.insert("c", "b", &json!({"score": 2})).unwrap();
}

#[test]
fn cumulative_document_read_limits_apply_to_all_writable_backends() {
    check_writable_backends(check_limits, 0);
}

#[test]
fn cumulative_document_write_limits_apply_to_all_writable_backends() {
    check_writable_backends(check_write_limits, 3);
}

fn check_write_limits(db: &DocumentDatabase<'_>) {
    let mode = DocumentTransactionMode::ReadSet;
    let limits = |units, bytes| DocumentRequestLimits {
        transaction: DocumentTransactionLimits {
            reads: DocumentReadLimits {
                max_read_units: 100,
                max_read_bytes: usize::MAX,
            },
            max_validation_items: 0,
        },
        max_write_units: units,
        max_write_bytes: bytes,
    };
    let is_write_limit = |error: &io::Error| {
        error.kind() == io::ErrorKind::InvalidInput
            && error.to_string() == "document transaction write limit exceeded"
    };
    db.transaction_with_request_limits(limits(3, 9), mode, None, None, |tx| {
        tx.put_raw(b"w", b"old")?;
        tx.delete_raw(b"w")?;
        tx.put_raw(b"w", b"new")
    })
    .unwrap();
    for limit in [limits(2, 9), limits(3, 8)] {
        let rejected = db.transaction_with_request_limits(limit, mode, None, None, |tx| {
            tx.put_raw(b"w", b"old")?;
            tx.delete_raw(b"w")?;
            assert!(is_write_limit(&tx.put_raw(b"w", b"try").unwrap_err()));
            assert!(tx.get_raw(b"w")?.is_none());
            Ok(())
        });
        assert!(
            matches!(rejected, Err(TransactionFailure::Callback(error)) if is_write_limit(&error))
        );
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"w")).unwrap(),
            Some(b"new".to_vec())
        );
    }

    let mut read_limited = limits(1, 8);
    read_limited.transaction.reads.max_read_units = 0;
    let rejected = db.transaction_with_request_limits(read_limited, mode, None, None, |tx| {
        tx.put_raw(b"w", b"read")?;
        assert!(is_read_limit(&tx.get_raw(b"w").unwrap_err()));
        Ok(())
    });
    assert!(matches!(rejected, Err(TransactionFailure::Callback(error)) if is_read_limit(&error)));
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"w")).unwrap(),
        Some(b"new".to_vec())
    );

    let indexed = IndexQuery {
        equal: vec![IndexValue::Integer(3)],
        ..Default::default()
    };
    // One raw receipt, one index entry, and one document share the limit.
    let rejected =
        db.transaction_with_request_limits(limits(2, usize::MAX), mode, None, None, |tx| {
            tx.put_raw(b"receipt", b"indexed")?;
            assert!(is_write_limit(
                &tx.insert("c", "z", &json!({"score": 3})).unwrap_err()
            ));
            Ok(())
        });
    assert!(matches!(rejected, Err(TransactionFailure::Callback(error)) if is_write_limit(&error)));
    assert!(db.get("c", "z").unwrap().is_none());
    assert!(
        db.query_index("c", "score", &indexed)
            .unwrap()
            .documents
            .is_empty()
    );
    assert!(
        db.transaction(|tx| tx.get_raw(b"receipt"))
            .unwrap()
            .is_none()
    );

    db.transaction_with_request_limits(limits(3, usize::MAX), mode, None, None, |tx| {
        tx.put_raw(b"receipt", b"indexed")?;
        tx.insert("c", "z", &json!({"score": 3}))
    })
    .unwrap();
    assert_eq!(
        db.query_index("c", "score", &indexed).unwrap().documents[0].id,
        "z"
    );
    assert_eq!(
        db.transaction(|tx| tx.get_raw(b"receipt")).unwrap(),
        Some(b"indexed".to_vec())
    );
    db.transaction(|tx| {
        assert!(tx.delete("c", "z")?);
        tx.delete_raw(b"receipt")
    })
    .unwrap();
}

fn check_writable_backends(check: fn(&DocumentDatabase<'_>), authority_commits: u64) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let checkpoint_path = dir.path().join("checkpoint");
    let catalog = dir.path().join("retained");
    let store = Store::open(&path).unwrap();
    let db = DocumentDatabase::new(&store);
    seed_indexed_documents(&db);
    store.put(b"key", b"data").unwrap();
    check(&db);
    let digest = store.checkpoint_to(&checkpoint_path).unwrap();

    let retained_limits = RetainedStoreLimits {
        base: Limits {
            file_bytes: 16384,
            entries: 32,
            key_bytes: 4096,
            work_bytes: 16384,
        },
        index: IndexLimits {
            blocks: 8,
            anchor_bytes: 4096,
        },
        suffix: SuffixLimits {
            bytes: 16384,
            operations: 32,
            work: 16384,
        },
        overlay: OverlayLimits {
            entries: 32,
            key_bytes: 4096,
        },
    };
    let export = RetainedExportLimits {
        max_log_bytes: 16384,
        max_live_keys: 32,
        max_live_key_bytes: 4096,
        max_output_bytes: 16384,
    };
    ManagedRetainedStore::copy_from_store(store, &path, &catalog, export, retained_limits).unwrap();
    let checkpoint_store =
        retry_lock(|| CheckpointStore::open(&path, &checkpoint_path, &digest)).unwrap();
    check(&DocumentDatabase::with_checkpoint_store(&checkpoint_store));
    drop(checkpoint_store);
    let mut retained = ManagedRetainedStore::open(&catalog, retained_limits).unwrap();
    check(&DocumentDatabase::with_managed_retained_store(
        &mut retained,
    ));

    let authorized = dir.path().join("authorized");
    let authority = dir.path().join("authority");
    type Catalog = AuthorizedRetainedStore<FileRetainedAuthority>;
    let initial = Catalog::prepare_from_store(
        retry_lock(|| Store::open(&path)).unwrap(),
        &path,
        &authorized,
        export,
        retained_limits,
    )
    .unwrap();
    let mut retained = Catalog::register_prepared(
        &authorized,
        retained_limits,
        FileRetainedAuthority::create(&authority).unwrap(),
        initial,
    )
    .unwrap();
    check(&DocumentDatabase::with_authorized_retained_store(
        &mut retained,
    ));
    let current = FileRetainedAuthority::open(&authority)
        .unwrap()
        .read()
        .unwrap()
        .unwrap();
    assert_eq!(current.revision, initial.revision + 2 * authority_commits);
    assert!(current.pending.is_none());
    if authority_commits == 0 {
        assert_eq!(current, initial);
    } else {
        drop(retained);
        let mut reopened = Catalog::open(
            &authorized,
            retained_limits,
            FileRetainedAuthority::open(&authority).unwrap(),
        )
        .unwrap();
        let db = DocumentDatabase::with_authorized_retained_store(&mut reopened);
        assert_eq!(
            db.transaction(|tx| tx.get_raw(b"w")).unwrap(),
            Some(b"new".to_vec())
        );
        assert!(
            db.transaction(|tx| tx.get_raw(b"receipt"))
                .unwrap()
                .is_none()
        );
        assert!(db.get("c", "z").unwrap().is_none());
    }
}

#[test]
fn document_snapshot_read_limits_accumulate_across_pages_on_all_views() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let checkpoint = root.path().join("checkpoint");
    let store = Store::open(&path).unwrap();
    seed_indexed_documents(&DocumentDatabase::new(&store));
    let digest = store.checkpoint_to(&checkpoint).unwrap();

    let exercise = |snapshot: DocumentSnapshot<'_>| {
        let first_query = IndexQuery {
            limit: 1,
            ..Default::default()
        };
        let limits = DocumentReadLimits {
            max_read_units: 4,
            max_read_bytes: usize::MAX,
        };
        let assert_rejected = |error: io::Error| {
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert_eq!(error.to_string(), "document snapshot read limit exceeded");
        };
        let snapshot = snapshot.with_read_limits(limits);
        let first = snapshot.query_index("c", "score", &first_query).unwrap();
        assert_eq!(first.documents[0].id, "a");
        let next_query = IndexQuery {
            after: Some(first.next.unwrap()),
            ..first_query
        };
        assert_rejected(snapshot.query_index("c", "score", &next_query).unwrap_err());
        assert_rejected(snapshot.get("c", "a").unwrap_err());

        let snapshot = snapshot.with_read_limits(DocumentReadLimits {
            max_read_units: 3,
            ..limits
        });
        assert_rejected(
            snapshot
                .query_index(
                    "c",
                    "score",
                    &IndexQuery {
                        limit: 2,
                        ..Default::default()
                    },
                )
                .unwrap_err(),
        );

        let snapshot = snapshot.with_read_limits(DocumentReadLimits {
            max_read_bytes: 0,
            ..limits
        });
        assert_rejected(snapshot.get("c", "a").unwrap_err());
        let next = snapshot
            .with_read_limits(limits)
            .query_index("c", "score", &next_query)
            .unwrap();
        assert_eq!(next.documents[0].id, "b");
    };

    let snapshot = store.snapshot(32, 4096).unwrap();
    exercise(DocumentSnapshot::new(&snapshot));
    drop(snapshot);
    drop(store);

    let store = retry_lock(|| CheckpointStore::open(&path, &checkpoint, &digest)).unwrap();
    let snapshot = store.snapshot().unwrap();
    exercise(DocumentSnapshot::with_checkpoint_snapshot(&snapshot));
    drop(snapshot);
    drop(store);

    let view = retry_lock(|| CheckpointView::open(&path, &checkpoint, &digest)).unwrap();
    exercise(DocumentSnapshot::with_checkpoint_view(&view));
}

#[test]
fn planned_projection_and_aggregation_cross_public_index_page_byte_limit() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().join("store")).unwrap();
    let db = DocumentDatabase::new(&store);
    db.create_collection("c", CollectionSchema::default())
        .unwrap();
    db.create_index(
        "c",
        IndexDefinition {
            name: "score".into(),
            fields: vec![vec!["score".into()]],
            unique: false,
            partial: None,
        },
    )
    .unwrap();
    let document = json!({"score": 1, "payload": "x".repeat(1024 * 1024 - 64)});
    for id in 0..17 {
        db.insert("c", &format!("doc-{id:02}"), &document).unwrap();
    }

    let first = db
        .query_index("c", "score", &IndexQuery::default())
        .unwrap();
    assert_eq!(first.documents.len(), 16);
    let second = db
        .query_index(
            "c",
            "score",
            &IndexQuery {
                after: first.next,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(second.documents.len(), 1);
    assert_eq!(second.documents[0].id, "doc-16");

    let filter = DocumentFilter::Equal {
        path: vec!["score".into()],
        value: json!(1),
    };
    // One metadata read, one index page, 17 index keys and 17 documents.
    let budget = DocumentReadLimits {
        max_read_units: 36,
        max_read_bytes: usize::MAX,
    };
    let mode = DocumentTransactionMode::ReadSet;
    db.transaction_with_read_limits(budget, mode, None, None, |tx| {
        let result = tx.query_planned(
            "c",
            &DocumentQuery {
                filter: filter.clone(),
                projection: Some(vec![vec!["score".into()]]),
                limit: 17,
                max_scanned: 17,
                ..Default::default()
            },
        )?;
        assert_eq!(result.result.plan.scanned, 17);
        assert_eq!(result.result.documents.len(), 17);
        assert!(
            result
                .result
                .documents
                .iter()
                .all(|doc| doc.document == json!({"score": 1}))
        );
        Ok(())
    })
    .unwrap();
    db.transaction_with_read_limits(budget, mode, None, None, |tx| {
        let result = tx.aggregate_stats_planned(
            "c",
            &AggregateStatsRequest {
                filter: filter.clone(),
                group_by: None,
                fields: vec![vec!["score".into()]],
                max_scanned: 17,
            },
        )?;
        assert_eq!(result.result.groups[0].count, 17);
        assert_eq!(result.result.groups[0].fields[0].sum, 17);
        Ok(())
    })
    .unwrap();
}
