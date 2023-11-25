use rusqlite::{Connection, Result};
use std::fs;
use std::time::Instant;

use crate::graph_node::GraphNode;
use crate::table::{create_table, insert_graph_nodes, traverse};
use crate::utils::{generate_random_adj_list, generate_random_vecs};

mod graph_node;
mod table;
mod utils;

fn main() -> Result<()> {
    let ndim = 128;
    let nvec = 1_000_000;
    let degree = 64;
    let radius = 100.0;

    // If index exists, remove it
    let index_filaname: String = format!("idx_{}Kvecs_{}D_{}deg.db", nvec / 1_000, ndim, degree);
    if fs::metadata(&index_filaname).is_ok() {
        match fs::remove_file(&index_filaname) {
            Ok(_) => println!("Removed existing index file"),
            Err(_) => println!("Failed to remove existing index file"),
        }
    }

    // Open connection to index file and create table
    let mut conn = Connection::open(index_filaname)?;
    create_table(&conn)?;
    conn.pragma_update(None, "journal_mode", &"WAL").unwrap();

    // Generate random vectors and graph to bootstrap
    let now = Instant::now();
    let data = generate_random_vecs(ndim, nvec, radius);
    let graph = generate_random_adj_list(nvec, degree, nvec);
    println!(
        "Generated {} vectors in {}-D and {}-degree random graph in {}ms",
        nvec,
        ndim,
        degree,
        now.elapsed().as_millis()
    );

    // Initialize table with a random graph of nvec nodes using batch insertions
    let now = Instant::now();
    let insert_batch_size = 1_000;
    for chunk_start in (0..nvec).step_by(insert_batch_size) {
        let chunk_end = std::cmp::min(chunk_start + insert_batch_size, nvec);
        let mut graph_nodes: Vec<GraphNode> = vec![];
        for i in chunk_start..chunk_end {
            graph_nodes.push(GraphNode {
                guid: Some((i + 1) as u64),
                vector: data[i].clone(),
                adj_list: graph[i].clone(),
            });
        }
        insert_graph_nodes(&mut conn, graph_nodes)?;
    }
    println!(
        "Inserted {} nodes in {}ms using batches of size {}",
        nvec,
        now.elapsed().as_millis(),
        insert_batch_size
    );

    // Time traversal using many samples
    let start_row_id: u64 = 1;
    let hops: u32 = 50;
    let nsamples = 100;
    let now = Instant::now();
    for _ in 0..nsamples {
        traverse(&conn, start_row_id, hops)?;
    }
    println!(
        "Time for {} hops on {}-degree graph is {}ms based on {} samples",
        hops,
        degree,
        now.elapsed().as_millis() / nsamples,
        nsamples
    );
    Ok(())
}
