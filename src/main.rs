use rand::Rng;
use rand_distr::{Distribution, Normal};
use rusqlite::{Connection, Result};

#[derive(Debug)]
struct GraphNode {
    guid: Option<u64>,
    vector: Vec<u8>,
    adj_list: Vec<u32>,
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
    println!("Created table");
    Ok(())
}

fn vec_u32_to_u8(vec: Vec<u32>) -> Vec<u8> {
    let mut vec_u8: Vec<u8> = vec![];
    for x in vec {
        vec_u8.extend_from_slice(&x.to_le_bytes());
    }
    vec_u8
}

fn vec_u8_to_u32(vec: Vec<u8>) -> Vec<u32> {
    let mut vec_u32: Vec<u32> = vec![];
    for i in 0..vec.len() / 4 {
        let mut bytes: [u8; 4] = [0; 4];
        bytes.copy_from_slice(&vec[i * 4..(i + 1) * 4]);
        vec_u32.push(u32::from_le_bytes(bytes));
    }
    vec_u32
}

fn insert_graph_node(conn: &Connection, graph_node: GraphNode) -> Result<()> {
    conn.execute(
        "INSERT INTO IndexTable (guid, vector, adj_list) VALUES (?1, ?2, ?3)",
        (&graph_node.guid, &(graph_node.vector), vec_u32_to_u8(graph_node.adj_list)),
    )?;
    Ok(())
}

fn print_table(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare("SELECT vertex_id, guid, vector, adj_list FROM IndexTable")?;
    let iter = stmt.query_map([], |row| {
        Ok(GraphNode {
            guid: row.get(1)?,
            vector: row.get(2)?,
            adj_list: vec_u8_to_u32(row.get(3)?),
        })
    })?;

    for item in iter {
        println!("Found graph_node {:?}", item.unwrap());
    }
    Ok(())
}

fn generate_random_vecs(ndim: usize, nvec: usize, radius: f32) -> Vec<Vec<u8>> {
    assert!(radius > 0.0 && radius < 127.0);
    let mut thr_rng = rand::thread_rng();
    let normal: Normal<f32> = Normal::new(0.0, 1.0).unwrap();
    let mut data:Vec<Vec<u8>> = Vec::with_capacity(nvec);
    for _ in 0..nvec {
        let vec: Vec<f32> = (0..ndim).map(|_| normal.sample(&mut thr_rng)).collect();
        let norm = vec.iter().fold(0.0, |acc, x| acc + x * x).sqrt();
        data.push(vec.iter().map(|x| (((*x * radius)/ norm) + 127.0) as u8).collect());
    }
    data
}

fn generate_random_adj_list(nvec: usize, degree: usize) -> Vec<Vec<u32>> {
    let mut thr_rng = rand::thread_rng();
    (0..nvec).map(|_| {
        (0..degree).map(|_| {thr_rng.gen_range(0..nvec) as u32}).collect()
    }).collect()
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    create_table(&conn)?;

    let ndim = 4;
    let nvec = 10;
    let degree = 8;
    let radius = 100.0;
    let data = generate_random_vecs(ndim, nvec, radius);
    let graph = generate_random_adj_list(nvec, degree);

    for i in 0..nvec {
        insert_graph_node(&conn, GraphNode {
            guid: Some(i as u64),
            vector: data[i].clone(),
            adj_list: graph[i].clone(),
        })?;
    }
    
    print_table(&conn)?;
    Ok(())
}
