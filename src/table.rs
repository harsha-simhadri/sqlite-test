use rand::Rng;
use rusqlite::{Connection, Result};

use crate::utils::{vec_u32_to_u8, vec_u64_to_set_str, vec_u8_to_u32};
use crate::GraphNode;

pub fn create_table(conn: &Connection) -> Result<()> {
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

pub fn insert_graph_nodes(conn: &mut Connection, graph_nodes: Vec<GraphNode>) -> Result<()> {
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

#[allow(dead_code)]
pub fn print_table(conn: &Connection) -> Result<()> {
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

pub fn get_nodes_by_row_id(conn: &Connection, row_ids: &Vec<u64>) -> Result<Vec<GraphNode>> {
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

pub fn traverse(conn: &Connection, start_row_id: u64, hops: u32) -> Result<()> {
    let mut thr_rng = rand::thread_rng();
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
    Ok(())
}

pub fn get_num_rows(conn: &Connection) -> Result<u64> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM IndexTable")?;
    let mut iter = stmt.query_map([], |row| Ok(row.get(0)?))?;
    iter.next().unwrap()
}

pub fn insert_graph_node_with_back_edges(
    conn: &mut Connection,
    graph_node: GraphNode,
) -> Result<()> {
    let tx = conn.transaction()?;
    tx.execute(
        "INSERT INTO IndexTable (guid, vector, adj_list) VALUES (?1, ?2, ?3)",
        (
            &graph_node.guid,
            &(graph_node.vector),
            vec_u32_to_u8(&graph_node.adj_list),
        ),
    )?;

    // get the firt entry in back_edges
    let mut statement_str =
        "SELECT vertex_id, guid, vector, adj_list FROM IndexTable WHERE rowid IN ".to_string();
    statement_str.push_str(&vec_u64_to_set_str(
        &graph_node.adj_list.into_iter().map(|x| x as u64).collect(),
    ));

    let mut stmt = tx.prepare(&statement_str)?;

    let back_iter_result: Vec<(u64, Vec<u32>)> = stmt
        .query_map([], |row| {
            Ok((
                row.get::<usize, u64>(0)?,
                vec_u8_to_u32(&row.get::<usize, Vec<u8>>(3)?),
            ))
        })?
        .map(|x| x.unwrap())
        .collect();
    drop(stmt);

    for (row_id, mut adj_list) in back_iter_result.into_iter() {
        let random_idx: usize = rand::thread_rng().gen_range(0..adj_list.len());
        adj_list[random_idx] = graph_node.guid.unwrap() as u32;
        tx.execute(
            "UPDATE IndexTable SET adj_list = ?1 WHERE rowid = ?2",
            (vec_u32_to_u8(&adj_list), row_id),
        )?;
    }
    tx.commit()
}
