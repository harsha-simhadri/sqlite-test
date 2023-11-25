use rand::Rng;
use rusqlite::{Connection, Result};
use std::time::Instant;

use utils::{generate_random_adj_list, generate_random_vecs, vec_u32_to_u8, vec_u64_to_set_str, vec_u8_to_u32};
mod utils;

#[derive(Debug)]
struct GraphNode {
    guid: Option<u64>,
    vector: Vec<u8>,
    adj_list: Vec<u32>,
}

impl std::fmt::Display for GraphNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut adj_list_str = String::new();
        for x in &self.adj_list {
            adj_list_str.push_str(&format!("{}, ", x));
        }
        write!(
            f,
            "guid: {:?}, vector: {:?}, adj_list: {:?}",
            self.guid, self.vector, adj_list_str
        )
    }
}


fn create_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IndexTable (
            vertex_id INTEGER PRIMARY KEY,
            guid      INTEGER,
            vector    BLOB,
            adj_list  BLOB
        )",
        (),
    )?;
    println!("Created empty table");
    Ok(())
}

fn insert_graph_nodes(conn: &mut Connection, graph_nodes: Vec<GraphNode>) -> Result<()> {
    let tx = conn.transaction()?;
    for graph_node in graph_nodes.iter() {
        tx.execute(
            "INSERT INTO IndexTable (guid, vector, adj_list) VALUES (?1, ?2, ?3)",
            (
                &graph_node.guid,
                &(graph_node.vector),
                vec_u32_to_u8(&graph_node.adj_list),
            ),
        )?;
    }
    tx.commit()?;
    Ok(())
}

fn print_table(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("SELECT vertex_id, guid, vector, adj_list FROM IndexTable")?;
    let iter = stmt.query_map([], |row| {
        Ok(GraphNode {
            guid: row.get(1)?,
            vector: row.get(2)?,
            adj_list: vec_u8_to_u32(&row.get(3)?),
        })
    })?;

    for item in iter {
        println!("Found graph_node {:}", item.unwrap());
    }
    Ok(())
}

fn get_nodes_by_row_id(conn: &Connection, row_ids: &Vec<u64>) -> Result<Vec<GraphNode>> {
    let mut statement_str =
        "SELECT vertex_id, guid, vector, adj_list FROM IndexTable WHERE rowid IN".to_string();
    statement_str.push_str(&vec_u64_to_set_str(row_ids));
    let mut stmt = conn.prepare(&statement_str)?;
    let iter = stmt.query_map([], |row| {
        Ok(GraphNode {
            guid: row.get(1)?,
            vector: row.get(2)?,
            adj_list: vec_u8_to_u32(&row.get(3)?),
        })
    })?;

    iter.collect()
}

fn time_traverse(conn: &Connection, start_row_id: u64, hops: u32) -> Result<u128> {
    let mut thr_rng = rand::thread_rng();
    let now = Instant::now();
    let mut next_row_ids: Vec<u64> = get_nodes_by_row_id(conn, &vec![start_row_id])?[0]
        .adj_list
        .iter()
        .map(|x| *x as u64)
        .collect();

    for _ in 0..hops {
        let nodes = get_nodes_by_row_id(conn, &next_row_ids)?;
        let _vectors: Vec<Vec<u8>> = nodes
            .iter()
            .map(|x| x.vector.clone())
            .collect::<Vec<Vec<u8>>>();
        let random_nbr: usize = thr_rng.gen_range(0..nodes.len());
        next_row_ids = nodes[random_nbr]
            .adj_list
            .iter()
            .map(|x| *x as u64)
            .collect();
    }

    Ok(now.elapsed().as_millis())
}

fn main() -> Result<()> {
    //let conn = Connection::open_in_memory()?;
    let mut conn = Connection::open("index1M.db")?;
    create_table(&conn)?;
    conn.pragma_update(None, "journal_mode", &"WAL").unwrap();

    let ndim = 128;
    let nvec = 1_000_000;
    let degree = 64;
    let radius = 100.0;

    let now = Instant::now();
    let data = generate_random_vecs(ndim, nvec, radius);
    let graph = generate_random_adj_list(nvec, degree);
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
    let mut total_time: u128 = 0;
    for _ in 0..nsamples {
        total_time += time_traverse(&conn, start_row_id, hops)?;
    }
    println!(
        "Time for {} hops on {}-degree graph is {}ms based on {} samples",
        hops,
        degree,
        total_time / nsamples,
        nsamples
    );
    Ok(())

    // Add new rows and update existing rows with back pointers.
}
