use rusqlite::{Connection, Result};
use std::fs;
use std::time::Instant;

use crate::graph_node::GraphNode;
use crate::table::{
    create_table, get_nodes_by_row_id, get_num_rows, insert_graph_node_with_back_edges,
    insert_graph_nodes, traverse,
};
use crate::utils::{
    generate_random_adj_list, generate_random_adj_lists, generate_random_vec, generate_random_vecs,
};

mod graph_node;
mod table;
mod utils;

fn main() -> Result<()> {
    let ndim = 128;
    let nvec = 10_000;
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
    let graph = generate_random_adj_lists(nvec, degree, nvec);
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

    // Add new nodes with back edges
    let now: Instant = Instant::now();
    let num_new_nodes = 10;
    for _ in 0..num_new_nodes {
        let num_rows = get_num_rows(&conn).unwrap();
        let new_guid = 1 + num_rows;
        insert_graph_node_with_back_edges(
            &mut conn,
            GraphNode {
                guid: Some(new_guid),
                vector: generate_random_vec(ndim, radius),
                adj_list: generate_random_adj_list(degree, num_rows as usize),
            },
        )?;
    }
    println!(
        "Inserted {} new nodes with back edges in {}ms",
        num_new_nodes,
        now.elapsed().as_millis()
    );

    // Make sure back edges were added for new nodes
    let num_rows = get_num_rows(&conn).unwrap();
    let nodes = get_nodes_by_row_id(&conn, &(num_rows + 1 - num_new_nodes..num_rows).collect());
    for node in nodes.unwrap() {
        let nghrs =
            get_nodes_by_row_id(&conn, &node.adj_list.iter().map(|x| *x as u64).collect()).unwrap();
        for ngbr in nghrs {
            assert!(ngbr.adj_list.contains(&(node.guid.unwrap() as u32)));
            // This test will fail occasionally even if all code is correct because of random collisions
        }
    }
    println!("Back edge check OK.");

    Ok(())
}
